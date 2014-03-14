using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;
using System.Web.Http;
using Graphene.Mongo.Reporting;
using Graphene.Reporting;

namespace Graphene.API.Controllers
{
    public class GrapheneReportController : ApiController
    {
        public ITrackerReportResults Post(IReportSpecification reportSpecification)
        {
            var mongoReportGenerator = new MongoReportGenerator(ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString);
            return mongoReportGenerator.GeneratorReport(reportSpecification);
        }
    }
}