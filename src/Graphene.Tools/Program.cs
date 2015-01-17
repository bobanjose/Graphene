using System;
using System.Collections.Generic;
using System.Configuration;
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
            var mongoConnectionString = string.Empty;
            var targetSqlConnectionString = string.Empty;
            var startDate = DateTime.MinValue;
            var endDate = DateTime.MaxValue;
            var daysInSpan = -1;

            var deleteRecordAfterMigration = false;

            if (!arguments.ContainsKey("mc"))
            {
                Console.WriteLine("Missing prameter mc with Mongo Source DB Connection String");
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
                Console.WriteLine("Missing prameter sc with SQL Connection String");
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
                    Console.WriteLine("Invalid value for paramter deleteaftermigration");
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
                    Console.WriteLine("Invalid value for paramter startdate");
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
                    Console.WriteLine("Invalid value for paramter enddate");
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
                    Console.WriteLine("Invalid value for paramter timespansindays");
                    paraValid = false;
                }
            }

            if (paraValid)
            {
                ILogger _logger = new MigrateConsoleLogger();
                var _mongoToSqlServerMigrator = new MongoToSQLServer(targetSqlConnectionString, mongoConnectionString, _logger);
                Console.CancelKeyPress += delegate
                {
                    _mongoToSqlServerMigrator.Stop();
                };

                _mongoToSqlServerMigrator.Start(deleteRecordAfterMigration, startDate, endDate, daysInSpan);
            }

            Console.ReadLine();
        }
    }

}
