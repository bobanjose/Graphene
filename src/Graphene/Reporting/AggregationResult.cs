using System;
using System.Collections.Generic;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public class AggregationResults<T>
     where T : ITrackable, new()
    {
        public AggregationResults()
        {
            Results = new List<AggregationResult<T>>();
        }

        public List<AggregationResult<T>> Results { get; private set; }
        public ReportResolution Resolution { get; internal set; }
    }

    public class AggregationResult<T>
        where T : ITrackable, new()
    {
        public AggregationResult()
        {
            Tracker = new T();
        }

        public DateTime MesurementTimeUtc { get; internal set; }
        public long Occurrence { get; internal set; }
        public long Total { get; internal set; }
        public T Tracker { get; private set; }
    }
}