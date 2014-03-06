using System;
using System.Collections.Generic;

namespace Graphene.Reporting
{
    public interface IReportSpecification
    {
        IEnumerable<IFilterConditions> FilterCombinations { get; }

        IEnumerable<string> Counters { get; }

        DateTime FromDateUtc { get; }

        DateTime ToDateUtc { get; }

        string TrackerTypeName { get; }

    }
    public interface IFilterConditions
    {
        IEnumerable<string> Filters { get; }
    }
}