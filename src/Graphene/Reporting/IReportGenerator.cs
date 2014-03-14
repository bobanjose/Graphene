using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Reporting
{
    public interface IReportGenerator
    {
        /*IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification);*/

        

        ITrackerReportResults GeneratorReport(IReportSpecification specification);

    }
}
