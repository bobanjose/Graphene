using System;
using System.Linq;
using System.Collections.Generic;

namespace Graphene.Reporting
{    
    public interface IQueryResults
    {
        DateTime MesurementTimeUtc { get; }
        Dictionary<string, long> MeasurementValues { get; }
    }

}