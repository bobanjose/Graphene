﻿using System;
using System.Configuration;
using System.Reflection;
using System.Web.Http;
using Autofac;
using Autofac.Integration.WebApi;
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

            var offsetHours = 0;
            var useBuckets = false;
            TimespanRoundingMethod roundingMethod;
            ReportSourceType reportSourceType;


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
                var minBackoff = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_PersistMinBackoff"], 1);
                var maxBackoff = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_PersistMaxBackoff"], 30);
                var deltaBackoff = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_PersistDeltaBackoff"], 2);
                var initialRetry = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_ReportInitialRetryTime"], 200);
                var incrementalRetry = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_ReportIncrementalRetryTime"], 400);
                var commandTimeout = tempNumber.TryParse(ConfigurationManager.AppSettings["SQLServer_ReportCommandTimeout"], 300);

                builder.Register(x =>
                {
                    var _logger = x.Resolve<ILogger>();
                    var sqlPersister =
                        new PersistToSQLServer(
                            _sqlConnectionStringSettings.ConnectionString, _logger, maxRetries: maxRetries, minBackoff: minBackoff, maxBackoff: maxBackoff, deltaBackoff: deltaBackoff);
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

            Configurator.Initialize(new Settings
            {
                Persister = container.Resolve<IPersist>(),
                DefaultReportSource = Enum.TryParse(ConfigurationManager.AppSettings["DefaultReportSource"], out reportSourceType) ? reportSourceType : ReportSourceType.SQLReportGenerator,
                GrapheneRoundingMethod = Enum.TryParse(ConfigurationManager.AppSettings["RoundingMethod"], out roundingMethod) ? roundingMethod : TimespanRoundingMethod.Start,
                UseBuckets = bool.TryParse(ConfigurationManager.AppSettings["UseBuckets"], out useBuckets) ? useBuckets : false,
                DayTotalTZOffset = int.TryParse(ConfigurationManager.AppSettings["MidnightOffsetForTotals"], out offsetHours) ? getDayTotalTimeZoneOffset(offsetHours) : getDayTotalTimeZoneOffset(8)
            });

            // Create the depenedency resolver.
            var resolver = new AutofacWebApiDependencyResolver(container);

            GlobalConfiguration.Configuration.DependencyResolver = resolver;
            
            container.Resolve<ILogger>().Info("Autofac Registered");
        }

        private static TimeSpan getDayTotalTimeZoneOffset(int offsetHours)
        {
            TimeSpan dayTotalTzOffset;
            if (offsetHours >= -11 && offsetHours <= 14)
            {
                dayTotalTzOffset = new TimeSpan(offsetHours, 0, 0);
            }
            else
            {
                dayTotalTzOffset = new TimeSpan(0, 0, 0);
            }
            return dayTotalTzOffset;
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