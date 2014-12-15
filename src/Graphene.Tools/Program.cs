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
            var targetMongoConnectionString = string.Empty;

            var stopAfter = -1;
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

            if (arguments.ContainsKey("stopafter"))
            {
                try
                {
                    stopAfter = Convert.ToInt32(arguments["stopafter"]);
                }
                catch
                {
                    Console.WriteLine("Invaid value for paramter stopafter");
                    paraValid = false;
                }
            }

            if (arguments.ContainsKey("deleteaftermigration"))
            {
                try
                {
                    deleteRecordAfterMigration = Convert.ToBoolean(arguments["deleteaftermigration"]);
                }
                catch
                {
                    Console.WriteLine("Invaid value for paramter deleteaftermigration");
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

                _mongoToSqlServerMigrator.Start(deleteRecordAfterMigration, stopAfter);
            }

            Console.ReadLine();
        }
    }

}
