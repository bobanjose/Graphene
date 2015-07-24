using System;
using System.Collections.Generic;
using Graphene.Reporting;

namespace Graphene.API.Models
{
    public class JsonReportSpecification : IReportSpecification
    {
        public IEnumerable<IFilterConditions> FilterCombinations { get; set; }
        public IEnumerable<IMeasurement> Counters { get; set; }
        public DateTime FromDateUtc { get; set; }
        public DateTime ToDateUtc { get; set; }
        public IEnumerable<string> TypeNames { get; set; }
        public ReportResolution Resolution { get; set; }
        public TimeSpan OffsetTotalsByHours { get; set; }
        public IEnumerable<IFilterConditions> ExcludeFilters { get; set; }
    }
}