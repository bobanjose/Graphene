using System;
using System.Collections.Generic;

namespace Graphene.Reporting
{
    public class AggregationResults<T>
    {
        public AggregationResults()
        {
            Results = new List<AggregationResult<T>>();
        }

        public List<AggregationResult<T>> Results { get; private set; }
        public ReportResolution Resolution { get; internal set; }
    }

    public class AggregationResult<T>
    {
        public AggregationResult()
        {
            Tracker = (T) Activator.CreateInstance(typeof (T));
        }

        public DateTime MesurementTimeUtc { get; internal set; }
        public long Occurrence { get; internal set; }
        public long Total { get; internal set; }
        public T Tracker { get; private set; }
    }
}