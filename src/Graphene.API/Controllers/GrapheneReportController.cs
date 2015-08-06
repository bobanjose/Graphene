using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Graphene.Configuration;
using Graphene.Reporting;

namespace Graphene.API.Controllers
{
    public class GrapheneReportController : ApiController
    {
        private readonly ILogger _logger;
        private readonly IEnumerable<IReportGenerator> _reportGenerator;

        public GrapheneReportController(ILogger logger, IEnumerable<IReportGenerator>  reportGenerator)
        {
            _logger = logger;
            _reportGenerator = reportGenerator;
        }

        public ITrackerReportResults Post(IReportSpecification reportSpecification)
        {
            var reportGenerator = getReportGenerator(reportSpecification.ReportSourceType);
            var returnResult = reportGenerator.BuildReport(reportSpecification);
            return returnResult;
        }

        private IReportGenerator getReportGenerator(ReportSourceType? reportSourceType)
        {
            IReportGenerator reportGenerator = null;
            if (reportSourceType.HasValue)
            {
                reportGenerator = getReportGeneratorByType(reportSourceType.ToString());
            }

            if (reportGenerator == null)
            {
                reportGenerator = _reportGenerator.Count() == 1
                    ? _reportGenerator.FirstOrDefault()
                    : getReportGeneratorByType(Configurator.DefaultReportSource.ToString());
            }

            return reportGenerator;
        }

        private IReportGenerator getReportGeneratorByType(string reportSourceType)
        {
            return _reportGenerator.FirstOrDefault(
                report =>
                    string.Equals(report.GetType().Name, reportSourceType,
                        StringComparison.OrdinalIgnoreCase));
        }
    }
}