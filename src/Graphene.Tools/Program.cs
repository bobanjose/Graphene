using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Security.AccessControl;
using Graphene.Configuration;
using Graphene.Tools.Migrate;

namespace Graphene.Tools
{
    class Program
    {
        static void Main(string[] args)
        {
            var arguments = new Dictionary<string, string>();

            foreach (string argument in args)
            {
                string[] splitted = argument.Split("=".ToCharArray(), 2);

                if (splitted.Length == 2)
                {
                    arguments[splitted[0]] = splitted[1];
                }
            }
            var paraValid = true;
            string mongoConnectionString;
            string targetSqlConnectionString;
            var startDate = DateTime.MinValue;
            var endDate = DateTime.MaxValue;
            var daysInSpan = -1;

            var deleteRecordAfterMigration = false;
            var logFilePath = ConfigurationManager.AppSettings["LogFileLocation"];
            Boolean appendLog;
            Boolean.TryParse(ConfigurationManager.AppSettings["AppendLogs"], out appendLog);
            if (String.IsNullOrEmpty(logFilePath))
            {
                logFilePath = @"E:\Logs\GrapheneMigration.log"; 
            }
            var logStream = new StreamWriter(logFilePath,appendLog);
            logStream.AutoFlush = true;

            if (!arguments.ContainsKey("mc"))
            {
                logStream.WriteLine("Parameter mc with Mongo Source DB Connection String not provided on command line. Checking config file.");
                mongoConnectionString = ConfigurationManager.AppSettings["MongoDBConnectionString"];
                if (String.IsNullOrEmpty(mongoConnectionString) ||
                    String.IsNullOrWhiteSpace(mongoConnectionString))
                {
                    paraValid = false;
                }
            }
            else
            {
                mongoConnectionString = arguments["mc"];
            }

            if (!arguments.ContainsKey("sc"))
            {
                logStream.WriteLine("Parameter sc with SQL Connection String not provided on command line. Checking config file.");
                targetSqlConnectionString = ConfigurationManager.AppSettings["MSSQLConnectionString"];
                if (String.IsNullOrEmpty(targetSqlConnectionString) ||
                    String.IsNullOrWhiteSpace(targetSqlConnectionString))
                {
                    paraValid = false;
                }
            }
            else
            {
                targetSqlConnectionString = arguments["sc"];
            }

            if (arguments.ContainsKey("deleteaftermigration"))
            {
                try
                {
                    deleteRecordAfterMigration = Convert.ToBoolean(arguments["deleteaftermigration"]);
                }
                catch
                {
                    logStream.WriteLine("Invalid value for paramter deleteaftermigration");
                    paraValid = false;
                }
            }

            if (arguments.ContainsKey("startdate"))
            {
                try
                {
                    startDate = Convert.ToDateTime(arguments["startdate"]);
                }
                catch
                {
                    logStream.WriteLine("Invalid value for paramter startdate");
                    paraValid = false;
                }
            }

            if (arguments.ContainsKey("enddate"))
            {
                try
                {
                    endDate = Convert.ToDateTime(arguments["enddate"]);
                }
                catch
                {
                    logStream.WriteLine("Invalid value for paramter enddate");
                    paraValid = false;
                }
            }

            if (arguments.ContainsKey("timespansindays"))
            {
                try
                {
                    daysInSpan = Convert.ToInt32(arguments["timespansindays"]);
                }
                catch
                {
                    logStream.WriteLine("Invalid value for paramter timespansindays");
                    paraValid = false;
                }
            }

            if (paraValid)
            {
                var _mongoToSqlServerMigrator = new MongoToSQLServer(targetSqlConnectionString, mongoConnectionString, logStream);
                Console.CancelKeyPress += delegate
                {
                    _mongoToSqlServerMigrator.Stop();
                };

                _mongoToSqlServerMigrator.Start(deleteRecordAfterMigration, startDate, endDate, daysInSpan);
            }

        }
    }

}
