using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Vertica.Data.VerticaClient;
using System.Drawing;


namespace BabyNI
{
    public class dbConnection
    {
       
        public dbConnection()
        {
                
        }
      
        public string ConnectionString()
        {
            VerticaConnectionStringBuilder builder = new();

            builder.Host = "10.10.4.231";
            builder.Database = "test";
            builder.Port = 5433;
            builder.User = "bootcamp2";
            builder.Password = "bootcamp22023";

            VerticaConnection _conn = new(builder.ToString());


            return builder.ToString();

            
        }
       
    }
}
