using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;

namespace Graphene.Reporting
{

    public interface ITrackerReportResults
    {

        DateTime FromDateUtc { get; }

        DateTime ToDateUtc { get; }
        IEnumerable<IAggregationResult> AggregationResults { get; } 
        ReportResolution resolution { get; }

        IAggregationResult AddAggregationResult(DateTime mesurementTimeUtc, long occurence, long total);



    }


    public interface IAggregationResult
    {
      

        ushort TimeSlice { get; set; }

        DateTime MesurementTimeUtc { get;  }
        IEnumerable<IMeasurementResult> MeasurementValues { get;  }

        IMeasurementResult AddMeasurementResult(IMeasurement measurement, string value);

        long Occurence { get; set; }

        long Total { get; set; }
    }



}