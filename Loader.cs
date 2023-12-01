using System;
using Vertica.Data.VerticaClient;

namespace BabyNI
{

 
     public class Loader
    {
            private readonly string sourceFolderPath = "C:\\Users\\User\\Desktop\\babyNI\\ToBeLoaded";
            private readonly string finishedPath = "C:\\Users\\User\\Desktop\\babyNI\\LoadedData";

            public Loader()
            {
                Load();
            }

        public void Load()
        {
            foreach (string filePath in Directory.GetFiles(sourceFolderPath, "*.csv"))
            {
                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
                string fileName = Path.GetFileName(filePath);
               
                if (isFileLoaded(fileNameWithoutExtension))
                    
                { Console.WriteLine(fileNameWithoutExtension + " is already loaded. Skipping...."); }

                else {
                   try
                    {
                        dbConnection _conn = new();
                        using (VerticaConnection conn = new VerticaConnection(_conn.ConnectionString()))
                        {


                            string tableName;



                            if (fileNameWithoutExtension.Contains("RADIO_LINK_POWER"))
                                tableName = "TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER ";
                            else
                                tableName = "TRANS_MW_ERC_PM_WAN_RFINPUTPOWER ";

                            string copySql = $"COPY {tableName} FROM LOCAL '{filePath}' DELIMITER ',' SKIP 1  REJECTED DATA 'C:\\Users\\User\\Desktop\\babyNI\\rejected.txt' EXCEPTIONS 'C:\\Users\\User\\Desktop\\babyNI\\exceptions.txt' DIRECT;";
                            Console.WriteLine($"SQL Command: {copySql}");


                            conn.Open();

                            VerticaCommand cmd = new VerticaCommand(copySql, conn);
                            cmd.ExecuteNonQuery();
conn.Close();

                           
                        } 
                        Update(fileNameWithoutExtension);

                        if (IsFileAgg(getDate(fileNameWithoutExtension))) Console.WriteLine("File already aggregated");

                        else insertAgg(getDate(fileNameWithoutExtension));
                    }
                    catch (Exception ex)
                    {
                        dbConnection _conn = new();
                        using (VerticaConnection connection = new(_conn.ConnectionString()))
                        {

                            connection.Open();
                            using (var command = connection.CreateCommand())
                            {
                                command.CommandText = $"INSERT INTO TransactionLogError (FileName,errorMessage) VALUES ('{fileNameWithoutExtension}' ,'{ex.Message}')";
                                command.ExecuteNonQuery();
                            }
                            connection.Close();
                        }
                    }
                    if (fileName.Contains("RADIO_LINK"))
                    {
                        insertAgg(getDate(fileNameWithoutExtension));
                    }
 Update(fileNameWithoutExtension);
                    File.Move(filePath, Path.Combine(finishedPath, fileName));
                    
                }
            }Aggregator aggregator = new Aggregator();
                    aggregator.Aggregate();
                   
        }
        private bool isFileLoaded(string fileNameWithoutExtension)
        {
            dbConnection _conn = new dbConnection();

            using (VerticaConnection connect = new VerticaConnection(_conn.ConnectionString()))
            {
                connect.Open();

                string sqlCommand = $"SELECT isLoaded FROM TransactionLogStatus WHERE fileName = '{fileNameWithoutExtension}'";

                using (VerticaCommand command = new VerticaCommand(sqlCommand, connect))
                {
                    using (VerticaDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return Convert.ToBoolean(reader["isLoaded"]);
                        }
                    }
                }

                connect.Close();
            }
            return false; // Default to false if no record is found
        }

        private void Update(string fileNameWithoutExtension)
        {
            dbConnection _conn = new dbConnection();
            using (VerticaConnection connection = new VerticaConnection(_conn.ConnectionString()))
            {

                connection.Open();
                using (var cmd = connection.CreateCommand())
                {
                    //update TransactionLogStatus
                    string updateLog = $"UPDATE TransactionLogStatus SET isloaded = 'true', dateLoaded = now() WHERE fileName = '{fileNameWithoutExtension}' ";
                    cmd.CommandText = updateLog;
                    cmd.ExecuteNonQuery();
                }



                connection.Close();

            }
        }

        private void insertAgg(DateTime datetime_key)
        {
            using (VerticaConnection connection = new VerticaConnection())
            {
                dbConnection _conn = new dbConnection();
                connection.ConnectionString = _conn.ConnectionString();

                connection.Open();
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"INSERT INTO AggregationLog (DATETIME_KEY, isAggregated, dateAgg) VALUES ('{datetime_key}', 'false', null)";
                    cmd.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        private bool IsFileAgg(DateTime datetime_key)
        {
            dbConnection _conn = new dbConnection();

            using (VerticaConnection connect = new VerticaConnection(_conn.ConnectionString()))
            {
                connect.Open();

                string sqlCommand = $"SELECT isAggregated FROM  AggregationLog WHERE DATETIME_KEY= '{datetime_key}'";

                using (VerticaCommand command = new VerticaCommand(sqlCommand, connect))
                {
                    using (VerticaDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return Convert.ToBoolean(reader["isAggregated"]);
                        }
                    }
                }

                connect.Close();
            }
            return false; // Default to false if no record is found
        }

        private static DateTime getDate(string fileName)
        {

            string[] splittedEl = fileName.Split("_");

            string hour = splittedEl[splittedEl.Length - 1];
            string date = splittedEl[splittedEl.Length - 2];

            string dateTime = $"{date} {hour}";
            string dateFormat = "yyyyMMdd hhmmss";

            DateTime dateTime_Key = DateTime.ParseExact(dateTime, dateFormat, null);

            return dateTime_Key;

            throw new NotImplementedException();
        }
    }
}