using System;
using System.Collections.Generic;

namespace Graphene.Reporting
{
    public interface ITrackerReportResults
    {
        DateTime FromDateUtc { get; }

        DateTime ToDateUtc { get; }
        IEnumerable<IAggregationResult> AggregationResults { get; }
        ReportResolution resolution { get; }


    }

    public interface ITrackerReportResultsBuilder : ITrackerReportResults
    {
        IAggregationBuildableResult AddAggregationResult(DateTime? mesurementTimeUtc, string typeName, string keyFilter, long occurence, long total);
    }
}