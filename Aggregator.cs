using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Reflection.PortableExecutable;
using System.Text.RegularExpressions;
using Vertica.Data.VerticaClient;

namespace BabyNI
{
    public class Aggregator
    {
        private AppSettings _appSettings;
        public Aggregator()
        {
            Aggregate(); 
        }

        public void Aggregate() {

            _appSettings = LoadAppSettings();

           
            using (VerticaConnection connection = new(_appSettings.VerticaConnectionString))
            {

                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    string sqlHOURLY = "INSERT INTO TRANS_MW_AGG_SLOT_HOURLY" +
                   " SELECT" +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID," +
                   " date_trunc('HOUR', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                   " MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)," +
                   " MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)," +
                   " ABS(MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)) - ABS(MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)) AS RSL_DEVIATION" +
                   " FROM TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER" +
                   " INNER JOIN TRANS_MW_ERC_PM_WAN_RFINPUTPOWER ON TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.NETWORK_SID = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID" +
                   " INNER JOIN AggregationLog ON AggregationLog.DATETIME_KEY = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY" +
                   " WHERE AggregationLog.isAggregated = false" +
                   " GROUP BY" +
                   " date_trunc('HOUR', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID;";   
                    
                    command.CommandText = sqlHOURLY;
                    command.ExecuteNonQuery();
                }

                using (var cmd = connection.CreateCommand())
                {

                

                    string sqlDay= "INSERT INTO TRANS_MW_AGG_SLOT_DAILY" +
                   " SELECT" +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID," +
                   " date_trunc('Day', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                   " MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)," +
                   " MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)," +
                   " ABS(MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)) - ABS(MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)) AS RSL_DEVIATION" +
                   " FROM TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER" +
                   " INNER JOIN TRANS_MW_ERC_PM_WAN_RFINPUTPOWER ON TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.NETWORK_SID = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID" +
                   " INNER JOIN AggregationLog ON AggregationLog.DATETIME_KEY = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY" +
                   " WHERE AggregationLog.isAggregated = false" +
                   " GROUP BY" +
                   " date_trunc('Day', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                   " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID;";

                    cmd.CommandText = sqlDay;
                    cmd.ExecuteNonQuery();
                }
                connection.Close();


                Update(GETDate());

            }
        }


        private void Update(DateTime Datetime_key)
        {
        
            VerticaConnection conn = new VerticaConnection(_appSettings.VerticaConnectionString);

            conn.Open();

            // Update aggregation Log
            string updateAgg = $"UPDATE AggregationLog SET isAggregated = 'true', dateAgg = now() WHERE DATETIME_KEY = '{Datetime_key}'";
            VerticaCommand cmd = new VerticaCommand(updateAgg, conn); // Pass the SQL command and the connection
            cmd.ExecuteNonQuery();

            conn.Close();

        }

        private DateTime GETDate()
        {
         
            VerticaConnection connect = new VerticaConnection(_appSettings.VerticaConnectionString);

            try
            {
                connect.Open();

                string CommandText =
                    "SELECT" +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID," +
                    " date_trunc('Day', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                    " MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)," +
                    " MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)," +
                    " ABS(MAX(TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.RFInputPower)) - ABS(MAX(TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.MaxRxLevel)) AS RSL_DEVIATION" +
                    " FROM TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER" +
                    " INNER JOIN TRANS_MW_ERC_PM_WAN_RFINPUTPOWER ON TRANS_MW_ERC_PM_WAN_RFINPUTPOWER.NETWORK_SID = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID" +
                    " INNER JOIN AggregationLog ON AggregationLog.DATETIME_KEY = TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY" +
                    " WHERE AggregationLog.isAggregated = false" +
                    " GROUP BY" +
                    " date_trunc('Day', TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Time)," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.DATETIME_KEY," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NeAlias," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.Netype," +
                    " TRANS_MW_ERC_PM_TN_RADIO_LINK_POWER.NETWORK_SID;";

            using (VerticaCommand command = new VerticaCommand(CommandText, connect))
            {
                using (VerticaDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read()) // Check if there is at least one row
                    {
                        return Convert.ToDateTime(reader["DATETIME_KEY"]);
                    }
                }
            }

            // If no rows are found, you might want to handle this case (throw an exception or return a default DateTime value)
            throw new InvalidOperationException("No rows found in the result set.");
        }
    finally
    {
        connect.Close();
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
    }

            }
           
        

  