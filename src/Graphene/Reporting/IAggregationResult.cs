using System;
using System.Collections.Generic;

namespace Graphene.Reporting
{
    
    public interface IAggregationResult
    {
        string MeasurementName { get; set; }
        string MeasurementValue { get; set; }
    }

}