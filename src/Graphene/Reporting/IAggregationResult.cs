using System;
using System.Collections.Generic;

namespace Graphene.Reporting
{
    public interface IAggregationResult
    {
        ushort TimeSlice { get; set; }

        string TypeName { get; set; }
        
        DateTime MesurementTimeUtc { get; }
        IEnumerable<IMeasurementResult> MeasurementValues { get; }

        long Occurence { get; set; }

        long Total { get; set; }
    
    }

    public interface IAggregationBuildableResult : IAggregationResult
    {
    
        IMeasurementResult AddMeasurementResult(IMeasurement measurement, string value);
    }
}