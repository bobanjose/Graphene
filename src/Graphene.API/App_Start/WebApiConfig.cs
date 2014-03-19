using System;
using System.Collections;
using System.Collections.Generic;
using System.Web.Http;
using Graphene.API.Controllers;
using Graphene.API.Models;
using Graphene.Reporting;
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