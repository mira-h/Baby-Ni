using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Drawing;
using Vertica.Data.VerticaClient;
using System.Reflection.PortableExecutable;
using System.Buffers.Text;
using Vertica.Data.Internal.DotNetDSI.DataEngine;

namespace BabyNI
{
    public class Parser
    {

        private  AppSettings _appSettings;
        public void ParseTxtToCsv()

        {
            _appSettings = LoadAppSettings();
          
            // Define the output CSV file path

            foreach (string filePath in Directory.GetFiles(_appSettings.sourceFolderPath, "*.txt"))
            {

                Console.WriteLine($"Processing file: {filePath}");

                string fileName = Path.GetFileName(filePath);

                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);

                Dictionary<string, int> headers = new Dictionary<string, int>();

                string outputCsvFilePath = Path.Combine(_appSettings.destinationFolderPath, Path.GetFileNameWithoutExtension(filePath) + ".csv");



                List<Dictionary<string, string>> data = new List<Dictionary<string, string>>();

                if (IsFileParsed(fileNameWithoutExtension))
                {
                    Console.WriteLine($"File '{fileNameWithoutExtension}' has already been parsed. Skipping.");
                    continue;
                }
                else
                {
                    try
                    {
                        if (fileName.Contains("RADIO_LINK_POWER"))
                        {
                            using (StreamReader reader = new StreamReader(filePath))
                            {
                                string[] headersOriginal = reader.ReadLine().Split(',');

                                //New elements to add
                                string NETWORK_SID = "NETWORK_SID";
                                string DATETIME_KEY = "DATETIME_KEY";

                                // Combine the existing array with the new elements at the beginning
                                string[] headersArray = new[] { NETWORK_SID, DATETIME_KEY }.Concat(headersOriginal).ToArray();

                                headers.Add(NETWORK_SID, 0);

                                headers.Add(DATETIME_KEY, 1);
                                for (int i = 2; i < headersArray.Length; i++)
                                {
                                    headers[headersArray[i]] = i;
                                }

                                headers.Add("LINK", 21);
                                headers.Add("TID", 22);
                                headers.Add("FARENDTID", 23);
                                headers.Add("SLOT", 24);
                                headers.Add("PORT", 25);

                                while (!reader.EndOfStream)
                                {
                                    string[] values = reader.ReadLine().Split(',');

                                    Dictionary<string, string> row = new Dictionary<string, string>();

                                    foreach (KeyValuePair<string, int> header in headers)
                                    {
                                        if (header.Key == "NETWORK_SID")
                                        {
                                            row["NETWORK_SID"] = NetworkSid(values[6], values[7]);
                                        }
                                        else if (header.Key == "DATETIME_KEY")
                                        {
                                            string datetime = getDate(fileNameWithoutExtension).ToString();
                                            row["DATETIME_KEY"] = datetime;
                                        }

                                        else if (header.Key == "LINK")
                                        {
                                            if (values[2] == "Unreachable Bulk FC") row["LINK"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                string link = getLink(row["Object"]);
                                                row["LINK"] = link;
                                            }
                                        }

                                        else if (header.Key == "TID")
                                        {
                                            if (row["Object"] == "Unreachable Bulk FC") row["TID"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                string TID = getTID(row["Object"]);
                                                row["TID"] = TID;
                                            }

                                        }
                                        else if (header.Key == "FARENDTID")
                                        {
                                            if (row["Object"] == "Unreachable Bulk FC") row["FARENDTID"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                string FARENDTID = getFARENDTID(values[2]);
                                                row["FARENDTID"] = FARENDTID;
                                            }

                                        }

                                        else if (header.Key == "SLOT")


                                        {
                                            if (row["Object"] == "Unreachable Bulk FC") row["SLOT"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                row["SLOT"] = getSlotPort(row["LINK"])[0];

                                            }
                                        }

                                        else if (header.Key == "PORT")
                                        {

                                            if (row["Object"] == "Unreachable Bulk FC") row["PORT"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                row["PORT"] =  getSlotPort(row["LINK"])[getSlotPort(row["LINK"]).Length - 1];



                                            }
                                        }

                                        else row[(header.Key)] = values[(header.Value) - 2];



                                    }



                                    // Check if Object is "Unreachable Bulk FC" or FailureDescription is not "-"
                                    if (row["Object"] != "Unreachable Bulk FC" && row["FailureDescription"] == "-")
                                    {
                                        if (row["LINK"].Contains("+"))
                                        {

                                            Dictionary<string, string> row2 = new Dictionary<string, string>(row);



                                            row["SLOT"] = getSlotPort(row["LINK"])[0];
                                            data.Add(row);



                                            row2["SLOT"] = getSlotPort(row["LINK"])[1];
                                            data.Add(row2);


                                        }


                                        else data.Add(row);
                                    }

                                }

                            }




                          
                            using (VerticaConnection conn = new VerticaConnection(_appSettings.VerticaConnectionString))
                            {
                                string CfileName = fileName.Replace('_', ' ');

                                Console.WriteLine(CfileName);

                                conn.Open();

                                string sqlCommand = $"SELECT * FROM ColStatus WHERE '{CfileName}' LIKE '%' || fileName || '%' AND  isEnabled = true";

                                using (VerticaCommand command = new VerticaCommand(sqlCommand, conn))
                                {
                                    using (VerticaDataReader reader = command.ExecuteReader())
                                    {
                                        HashSet<string> enabledColumns = new HashSet<string>();

                                        while (reader.Read())
                                        {
                                            string columnName = reader["columnName"].ToString();

                                            enabledColumns.Add(columnName);
                                        }

                                        // Remove disabled columns from headers
                                        headers = headers.Where(pair => enabledColumns.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value);

                                        // Remove disabled columns from data
                                        data.ForEach(row => row.Keys.ToList().Where(k => !enabledColumns.Contains(k)).ToList().ForEach(k => row.Remove(k)));
                                    }
                                }

                                conn.Close();
                            }

                            using (StreamWriter writer = new StreamWriter(outputCsvFilePath, true))
                            {
                                // Write headers to the CSV file
                                writer.WriteLine(string.Join(",", headers.Keys));

                                // Write data to the CSV file
                                foreach (Dictionary<string, string> row in data)
                                {

                                    List<string> values = new List<string>();
                                    foreach (string header in headers.Keys)
                                    {
                                        values.Add(row[header]);
                                    }

                                    writer.WriteLine(string.Join(",", row.Values));
                                }
                            }

             

                            Update(fileNameWithoutExtension, getDate(Path.GetFileNameWithoutExtension(filePath)));
                            Console.WriteLine($"Processed file: {filePath}"); // Debug statement
                            File.Move(filePath, _appSettings.archive); 
                        }


                        else
                        {

                            using (StreamReader reader = new StreamReader(filePath))
                            {
                                string[] headersOriginal = reader.ReadLine().Split(',');

                                //New elements to add
                                string NETWORK_SID = "NETWORK_SID";
                                string DATETIME_KEY = "DATETIME_KEY";

                                // Combine the existing array with the new elements at the beginning
                                string[] headersArray = new[] { NETWORK_SID, DATETIME_KEY }.Concat(headersOriginal).ToArray();

                                headers.Add(NETWORK_SID, 0);

                                headers.Add(DATETIME_KEY, 1);
                                for (int i = 2; i < headersArray.Length; i++)
                                {
                                    headers[headersArray[i]] = i;
                                }


                                headers.Add("SLOT", 17);
                                headers.Add("PORT", 18);

                                while (!reader.EndOfStream)
                                {
                                    string[] values = reader.ReadLine().Split(',');

                                    Dictionary<string, string> row = new Dictionary<string, string>();

                                    foreach (KeyValuePair<string, int> header in headers)
                                    {
                                        if (header.Key == "NETWORK_SID")
                                        {
                                            row["NETWORK_SID"] = NetworkSid(values[6], values[7]);
                                        }
                                        else if (header.Key == "DATETIME_KEY")
                                        {
                                            string datetime = getDate(Path.GetFileNameWithoutExtension(filePath)).ToString();
                                            row["DATETIME_KEY"] = datetime;
                                        }


                                        else if (header.Key == "SLOT")
                                        {
                                            if (row["Object"] == "Unreachable Bulk FC") row["SLOT"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                // List<Tuple<string, string>> slotport = getSlotPort(row["LINK"]);

                                                row["SLOT"] = GetSlotPort(row["Object"]).Item1;

                                            }
                                        }

                                        else if (header.Key == "PORT")
                                        {

                                            if (row["Object"] == "Unreachable Bulk FC") row["PORT"] = "Unreachable Bulk FC";
                                            else
                                            {
                                                row["PORT"] = GetSlotPort(row["Object"]).Item2;



                                            }
                                        }

                                        else row[(header.Key)] = values[(header.Value) - 2];



                                    }



                                    // Check if FarEndTID is not "---" or FailureDescription is "-"
                                    if (row["FarEndTID"] != "----" && row["FailureDescription"] == "-")
                                    {
                                        data.Add(row);
                                    }

                                }

                            }




                          
                            using (VerticaConnection conn = new VerticaConnection(_appSettings.VerticaConnectionString))
                            {
                                string CfileName = fileName.Replace('_', ' ');

                                Console.WriteLine(CfileName);

                                conn.Open();

                                string sqlCommand = $"SELECT * FROM ColStatus WHERE '{CfileName}' LIKE '%' || fileName || '%' AND  isEnabled = true";

                                using (VerticaCommand command = new VerticaCommand(sqlCommand, conn))
                                {
                                    using (VerticaDataReader reader = command.ExecuteReader())
                                    {
                                        HashSet<string> enabledColumns = new HashSet<string>();

                                        while (reader.Read())
                                        {
                                            string columnName = reader["columnName"].ToString();

                                            enabledColumns.Add(columnName);
                                        }

                                        // Remove disabled columns from headers
                                        headers = headers.Where(pair => enabledColumns.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value);

                                        // Remove disabled columns from data
                                        data.ForEach(row => row.Keys.ToList().Where(k => !enabledColumns.Contains(k)).ToList().ForEach(k => row.Remove(k)));
                                    }
                                }

                                conn.Close();
                            }

                            using (StreamWriter writer = new StreamWriter(outputCsvFilePath, true))
                            {
                                // Write headers to the CSV file
                                writer.WriteLine(string.Join(",", headers.Keys));

                                // Write data to the CSV file
                                foreach (Dictionary<string, string> row in data)
                                {

                                    List<string> values = new List<string>();
                                    foreach (string header in headers.Keys)
                                    {
                                        values.Add(row[header]);
                                    }

                                    writer.WriteLine(string.Join(",", row.Values));
                                }
                            }

                            Update(fileNameWithoutExtension, getDate(Path.GetFileNameWithoutExtension(filePath)));

                           Console.WriteLine($"Processed file: {filePath}"); // Debug statement
                            


                            File.Move(filePath,_appSettings.archive);
                                }


                       }
                        


                    catch (Exception ex)
                    {
                        
                        using (VerticaConnection connection = new(_appSettings.VerticaConnectionString))
                        {

                            connection.Open();
                            using (var command = connection.CreateCommand())
                            {
                                command.CommandText = $"INSERT INTO TransactionLogError (FileName,errorMessage) VALUES ('{fileName}' ,'{ex.Message}')";
                                command.ExecuteNonQuery();
                            }
                            connection.Close();
                        }
                    }
                    File.Move(filePath, Path.Combine(_appSettings.parsedData, fileName));
                    Console.WriteLine("Finished ParseTxtToCsv"); // Debug statement
                }
                Loader loader = new Loader();
                loader.Load();
            }
        }
        private AppSettings LoadAppSettings()
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();

            var appSettings = new AppSettings();
            configuration.GetSection("AppSettings").Bind(appSettings);

            return appSettings;
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

        private string NetworkSid(string neAlias, string neType)
        {
            // Combine NeAlias and NeType into a single string
            string combinedString = $"{neAlias}{neType}";

            using (SHA256 sha256 = SHA256.Create())
            {
                // Compute the hash
                byte[] hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(combinedString));

                // Convert the hash to a positive integer
                int positiveHash = Math.Abs(BitConverter.ToInt32(hashBytes, 0));

                return positiveHash.ToString();
            }
            throw new NotImplementedException();
        }




        private bool IsFileParsed(string fileNameWithoutExtension)
        {
          

            using (VerticaConnection connect = new VerticaConnection(_appSettings.VerticaConnectionString))
            {
                connect.Open();

                string sqlCommand = $"SELECT isParsed FROM TransactionLogStatus WHERE fileName = '{fileNameWithoutExtension}'";

                using (VerticaCommand command = new VerticaCommand(sqlCommand, connect))
                {
                    using (VerticaDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return Convert.ToBoolean(reader["isParsed"]);
                        }
                    }
                }

                connect.Close();
            }

            return false; // Default to false if no record is found
        }

        private string getLink(string obj)
        {
            int idx = obj.IndexOf("_");
            string newObj = obj.Substring(0, idx);
            string[] objParts = newObj.Split("/");

            var dotIndex = objParts[1].IndexOf(".");
            var plusIndex = objParts[1].IndexOf("+");

            if (dotIndex == -1) // if middle has NO .
            {
                return objParts[1] + "/" + objParts[2];
            }
            else if (plusIndex == -1) // if middle has a . AND NO +
            {
                return objParts[1].Replace('.', '/');
            }
            else // if middle has a . AND a +

            {
                return "";
            }
        }
        private string getTID(string objectValue)
        {
            int pfrom = objectValue.IndexOf("_") + 1 + "_".Length;
            int pto = objectValue.LastIndexOf("_") - 1;
            return objectValue.Substring(pfrom, pto - pfrom);
        }
        private string getFARENDTID(string objectValue)
        {
            
                return objectValue.Substring(objectValue.LastIndexOf("_") + "_".Length);
           

            throw new NotImplementedException();
        }


        private string[] getSlotPort(string link)
        {
            string[] objParts = link.Split("/");

            string[] slots;
            string[] ports;

            if (objParts[0].Contains('+'))
            {
                slots = new string[2];
                slots = objParts[0].Split('+');
                ports = objParts[1].Split('/');
            }
            else
            {
                slots = new string[1];
                slots[0] = objParts[0];
                ports = objParts[1].Split('/');
            }

            // Concatenate the slots and ports arrays and return the result
            return slots.Concat(ports).ToArray();
        }

            private Tuple<string, string> GetSlotPort(string input)
        {
            string[] parts = input.Split('/');

            if (parts.Length >= 2)
            {
                string slot = $"{parts[0]}/{parts[1].Split('.')[0]}+";
                string port = parts[1].Split('.')[1];
                return new Tuple<string, string>(slot, port);
            }

            // Handle the case where input does not contain '/' or '.' as needed
            return null;
        }

        private void Update(string fileNameWithoutExtension, DateTime DATETIME_KEY) {

            using (VerticaConnection con = new VerticaConnection(_appSettings.VerticaConnectionString))
            {

                con.Open();

                using (var command = con.CreateCommand())
                {
                    string sqlCommando = $"UPDATE TransactionLogStatus SET isParsed = 'true', dateParsed = now(), DATETIME_KEY= '{DATETIME_KEY}' WHERE fileName = '{fileNameWithoutExtension}'";
                    command.CommandText = sqlCommando;
                    command.ExecuteNonQuery();

                }

                con.Close();
            }
        }

    }
}