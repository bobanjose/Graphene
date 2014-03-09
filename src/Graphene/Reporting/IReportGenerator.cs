using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Reporting
{
    public interface IReportGenerator
    {
        IEnumerable<IQueryResults> GeneratorReport(IReportSpecification specification);

    }
}
