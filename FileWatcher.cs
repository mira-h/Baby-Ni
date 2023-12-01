using Vertica.Data.Internal.ADO.Net;
using System.Data.SqlClient;
using System.Drawing;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Vertica.Data.Internal.DotNetDSI;
using Vertica.Data.VerticaClient;
using System.Transactions;

namespace BabyNI
{
    public class FileWatcher
    {
        private readonly AppSettings _appSettings;

        public FileWatcher()
        {
            // Load app settings from appsettings.json
            _appSettings = LoadAppSettings();

            using var watcher = new FileSystemWatcher(_appSettings.WatchFolderPath);

            watcher.NotifyFilter = NotifyFilters.Attributes
                                             | NotifyFilters.CreationTime
                                             | NotifyFilters.DirectoryName
                                             | NotifyFilters.FileName
                                             | NotifyFilters.LastAccess
                                             | NotifyFilters.LastWrite
                                             | NotifyFilters.Security
                                             | NotifyFilters.Size;

            watcher.Changed += OnChanged;

            watcher.Filter = "*.txt";
            watcher.IncludeSubdirectories = true;
            watcher.EnableRaisingEvents = true;

            Console.WriteLine("Press enter to exit.");
            Parser parserTrial = new();
            parserTrial.ParseTxtToCsv();  
            Console.ReadLine();
        }

        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType != WatcherChangeTypes.Changed)
            {
                return;
            }

            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(e.FullPath);

            if (IsFileParsed(fileNameWithoutExtension))
            {
                Console.WriteLine($"{fileNameWithoutExtension} is already parsed. Skipping...");
                File.Move(e.FullPath, Path.Combine(_appSettings.RedundantFolderPath, fileNameWithoutExtension));
            }
            else
            {
                Console.WriteLine($"{fileNameWithoutExtension} is not parsed. Continuing...");
                using (VerticaConnection connection = new VerticaConnection(_appSettings.VerticaConnectionString))
                {
                  

                    connection.Open();
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = $"INSERT INTO TransactionLogStatus (fileName, isParsed,dateParsed, isLoaded,dateLoaded ) VALUES ('{fileNameWithoutExtension}', 'false', null, 'false', null)";
                        cmd.ExecuteNonQuery();
                    }
                    connection.Close();
                }
                string fileName = Path.GetFileName(e.FullPath);


                File.Move(e.FullPath, Path.Combine(_appSettings.ToBeParsedFolderPath, fileName));
            }

            Console.WriteLine($"File Detected: {e.FullPath}");
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
        private bool IsFileParsed(string fileNameWithoutExtension)
        {
            dbConnection _conn = new dbConnection();

            using (VerticaConnection connect = new VerticaConnection(_conn.ConnectionString()))
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
    }
}