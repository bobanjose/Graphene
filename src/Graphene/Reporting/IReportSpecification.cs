using System;
using System.Collections.Generic;
using System.Linq;
using Graphene.Util;

namespace Graphene.Reporting
{
    public enum ReportResolution
    {
        Hour = 0,
        FifteenMinute = 1,
        FiveMinute = 3,
        Minute = 4,
        Day = 5,
        Month = 6,
        Year = 7
    }
    public enum ReportSourceType
    {
        MongoReportGenerator,
        SQLReportGenerator
    }

    public interface IReportSpecification
    {
        IEnumerable<IFilterConditions> FilterCombinations { get; }

        IEnumerable<IMeasurement> Counters { get; }

        DateTime FromDateUtc { get; }

        DateTime ToDateUtc { get; }

        IEnumerable<string> TypeNames { get; }

        ReportResolution Resolution { get; }

        TimeSpan OffsetTotalsByHours { get; }

        IEnumerable<IFilterConditions> ExcludeFilters { get; }

        ReportSourceType? ReportSourceType { get; set; }
    }

    public interface IFilterConditions
    {
        IEnumerable<string> Filters { get; }
    }
}