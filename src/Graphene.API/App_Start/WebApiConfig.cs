using System;
using System.Configuration;
using System.Reflection;
using System.Web.Http;
using Autofac;
using Autofac.Integration.WebApi;
using Graphene.API.Controllers;
using Graphene.API.Models;
using Graphene.Configuration;
using Graphene.Mongo.Publishing;
using Graphene.Mongo.Reporting;
using Graphene.Publishing;
using Graphene.Reporting;
using Graphene.SQLServer;
using Newtonsoft.Json.Converters;

namespace Graphene.API
{
    public static class WebApiConfig
    {
        private static ConnectionStringSettings _mongoConnectionStringSettings;
        private static ConnectionStringSettings _sqlConnectionStringSettings;

        public static void Register(HttpConfiguration config)
        {
            config.Routes.MapHttpRoute("DefaultApi", "api/{controller}/{id}", new {id = RouteParameter.Optional}
                );

            // Uncomment the following line of code to enable query support for actions with an IQueryable or IQueryable<T> return type.
            // To avoid processing unexpected or malicious queries, use the validation settings on QueryableAttribute to validate incoming queries.
            // For more information, visit http://go.microsoft.com/fwlink/?LinkId=279712.
            //config.EnableQuerySupport();

            // To disable tracing in your application, please comment out or remove the following line of code
            // For more information, refer to: http://www.asp.net/web-api
            config.EnableSystemDiagnosticsTracing();

            config.Formatters.JsonFormatter.Indent = true;
            config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new StringEnumConverter());
            config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new JsonConverterReportSpecificationConverter());
            config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new JsonConverterMeasurementConverter());
            config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new JsonConverterFilterCombinationsConverter());

            var builder = new ContainerBuilder();

            // Register the Web API controllers.
            builder.RegisterApiControllers(Assembly.GetExecutingAssembly());


            


            builder.RegisterType<GrapheneLog4NetLogger>().As<ILogger>();

            // Build the container.

            // Configure Web API with the dependency resolver.



            // Graphene.Configurator.Initialize(new Settings() { Persister = new PersistToMongo(container.Resolve<IConfiguration>().ReportingStoreConnectionString), Logger = container.Resolve<ILogger>() });

            _mongoConnectionStringSettings = ConfigurationManager.ConnectionStrings["MongoConnectionString"];
            if (_mongoConnectionStringSettings != null && !string.IsNullOrWhiteSpace(
                _mongoConnectionStringSettings.ConnectionString))
            {
                builder.Register(x =>
                {
                    var logger = x.Resolve<ILogger>();
                    var mongoPersister =
                        new PersistToMongo(
                            _mongoConnectionStringSettings.ConnectionString, logger);
                    return mongoPersister;
                }).As<IPersist>();

                builder.Register(x =>
                {
                    var logger = x.Resolve<ILogger>();
                    var mongoReportGenerator =
                        new MongoReportGenerator(
                            _mongoConnectionStringSettings.ConnectionString, logger);
                    return mongoReportGenerator;
                }).As<IReportGenerator>();
            }

            _sqlConnectionStringSettings = ConfigurationManager.ConnectionStrings["SQLServerConnectionString"];
            if (_sqlConnectionStringSettings != null && !string.IsNullOrWhiteSpace(
                _sqlConnectionStringSettings.ConnectionString))
            {
                var tempNumber = 5;
                var maxRetries = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_MaxRetries"], 5);
                var initialRetry = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_InitialRetryTime"], 200);
                var incrementalRetry = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_IncrementalRetryTime"], 400);
                var commandTimeout = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_CommandTimeout"], 300);

                builder.Register(x =>
                {
                    var _logger = x.Resolve<ILogger>();
                    var sqlPersister =
                        new PersistToSQLServer(
                            _sqlConnectionStringSettings.ConnectionString, _logger, maxRetries: maxRetries, initialRetry: initialRetry, incrementalRetry: incrementalRetry);
                    return sqlPersister;
                }).As<IPersist>();

                builder.Register(x =>
                {
                    var _logger = x.Resolve<ILogger>();
                    var sqlReportGenerator =
                        new SQLReportGenerator(
                            _sqlConnectionStringSettings.ConnectionString, _logger, maxRetries, initialRetry, incrementalRetry, commandTimeout);
                    return sqlReportGenerator;
                }).As<IReportGenerator>();
            }
            
            var container = builder.Build();

            // Create the depenedency resolver.
            var resolver = new AutofacWebApiDependencyResolver(container);

            GlobalConfiguration.Configuration.DependencyResolver = resolver;
            
            container.Resolve<ILogger>().Info("Autofac Registered");
        }
    }

    public class JsonConverterReportSpecificationConverter : CustomCreationConverter<IReportSpecification>
    {
        public override IReportSpecification Create(Type objectType)
        {
            return new JsonReportSpecification();
        }
    }

    public class JsonConverterFilterCombinationsConverter : CustomCreationConverter<IFilterConditions>
    {
        public override IFilterConditions Create(Type objectType)
        {
            return new JsonFilterCondition();
        }
    }

    public class JsonConverterMeasurementConverter : CustomCreationConverter<IMeasurement>
    {
        public override IMeasurement Create(System.Type objectType)
        {
            return new JsonMeasurement();
        }
    }

    public static class IntExtensions
    {
        public static int TryParse(this int number, string stringNumber, int defaultNumber)
        {
            var parseNumber = 0;
            return int.TryParse(stringNumber, out parseNumber) ? parseNumber : defaultNumber;
        }
    }
}