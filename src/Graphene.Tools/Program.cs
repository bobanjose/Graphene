using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Graphene.Tools.Migrate;

namespace Graphene.Tools
{
    class Program
    {
        private static MongoToSQLServer _mongoToSqlServerMigrator;

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
            var sqlConnectionString = string.Empty;
            var startAt = 0;
            var stopAfter = -1;
            var deleteRecordAfterMigration = false;

            if (!arguments.ContainsKey("mc"))
            {
                Console.WriteLine("Missing prameter mc with Mongo Connection String");
                paraValid = false;
            }
            else
            {
                mongoConnectionString = arguments["mc"];
            }

            if (!arguments.ContainsKey("sc"))
            {
                Console.WriteLine("Missing prameter sc with SQL Connection String");
                paraValid = false;
            }
            else
            {
                sqlConnectionString = arguments["sc"];
            }

            if (arguments.ContainsKey("startat"))
            {
                try
                {
                    startAt = Convert.ToInt32(arguments["startat"]);
                }
                catch
                {
                    Console.WriteLine("Invaid value for paramter startat");
                    paraValid = false;
                }
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
                _mongoToSqlServerMigrator = new MongoToSQLServer(sqlConnectionString, mongoConnectionString, new Logger());

                Console.CancelKeyPress += delegate
                {
                    _mongoToSqlServerMigrator.Stop();
                };

                _mongoToSqlServerMigrator.Start(deleteRecordAfterMigration, startAt, stopAfter, 0);
            }

            Console.ReadLine();
        }
    }

    class Logger:ILog
    {
        public void Info(string message)
        {
            Console.WriteLine(message);
        }

        public void Error(Exception exception)
        {
            Console.WriteLine("ERROR::" + exception.Message);
        }
    }
}
