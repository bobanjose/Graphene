using System;
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
            

            builder.Register(x =>
            {
                var _logger = x.Resolve<ILogger>();
                var mongoReportGenerator =
                    new MongoReportGenerator(
                        ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString, _logger);
                return mongoReportGenerator;
            }).As<IReportGenerator>();


            if (ConfigurationManager.ConnectionStrings["MongoConnectionString"] != null && !string.IsNullOrWhiteSpace(
                ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString))
            {
                builder.Register(x =>
                {
                    var _logger = x.Resolve<ILogger>();
                    var mongoPersister =
                        new PersistToMongo(
                            ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString, _logger);
                    return mongoPersister;
                }).As<IPersist>();
            }

            if (ConfigurationManager.ConnectionStrings["SQLServerConnectionString"] != null && !string.IsNullOrWhiteSpace(
                ConfigurationManager.ConnectionStrings["SQLServerConnectionString"].ConnectionString))
            {
                builder.Register(x =>
                {
                    var _logger = x.Resolve<ILogger>();
                    var sqlPersister =
                        new PersistToSQLServer(
                            ConfigurationManager.ConnectionStrings["SQLServerConnectionString"].ConnectionString, _logger);
                    return sqlPersister;
                }).As<IPersist>();
            }

            var container = builder.Build();

            // Create the depenedency resolver.
            var resolver = new AutofacWebApiDependencyResolver(container);

            GlobalConfiguration.Configuration.DependencyResolver = resolver;

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
}