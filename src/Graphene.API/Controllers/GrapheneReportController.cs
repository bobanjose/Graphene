using System.Web.Http;
using Graphene.Configuration;
using Graphene.Reporting;

namespace Graphene.API.Controllers
{
    public class GrapheneReportController : ApiController
    {
        private readonly ILogger _logger;
        private readonly IReportGenerator _reportGenerator;

        public GrapheneReportController(ILogger logger, IReportGenerator reportGenerator)
        {
            _logger = logger;
            _reportGenerator = reportGenerator;
        }

        public ITrackerReportResults Post(IReportSpecification reportSpecification)
        {
            
            var returnResult = _reportGenerator.BuildReport(reportSpecification);
            return returnResult;
        }
    }
}