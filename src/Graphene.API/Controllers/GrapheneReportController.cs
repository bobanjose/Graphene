using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using Graphene.Configuration;
using Graphene.Reporting;

namespace Graphene.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class GrapheneReportController : ControllerBase
    {
        private readonly Graphene.Configuration.ILogger _logger;
        private readonly IEnumerable<IReportGenerator> _reportGenerator;

        public GrapheneReportController(Graphene.Configuration.ILogger logger, IEnumerable<IReportGenerator>  reportGenerator)
        {
            _logger = logger;
            _reportGenerator = reportGenerator;
        }

        [HttpPost]
        public ActionResult<ITrackerReportResults> Post([FromBody] IReportSpecification reportSpecification)
        {
            var reportGenerator = getReportGenerator(reportSpecification.ReportSourceType);
            var returnResult = reportGenerator.BuildReport(reportSpecification);
            return Ok(returnResult);
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
                    : getReportGeneratorByType(Configurator.Configuration.DefaultReportSource.ToString());
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