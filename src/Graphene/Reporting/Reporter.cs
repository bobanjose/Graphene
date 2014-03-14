using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Graphene.Tracking;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Graphene.Reporting
{
    public class Reporter<T, T1>
        where T : struct
        where T1 : ITrackable
    {
        public static ReportResolution GetResolutionFromDates(DateTime fromUtc, DateTime toUtc)
        {
            var resolution = ReportResolution.Hour;

            var reportSpan = toUtc.Subtract(fromUtc).TotalDays;

            if (reportSpan > 3600)
                resolution = ReportResolution.Year;
            else if (reportSpan >= 180)
                resolution = ReportResolution.Month;
            else if (reportSpan >= 10)
                resolution = ReportResolution.Day;
            else if (reportSpan >= 1)
                resolution = ReportResolution.Hour;
            else
                resolution = ReportResolution.Minute;

            return resolution;
        }

        public static AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc, ReportSpecification<T, T1> reportSpecs)
        {
            var aggregationResults = Configurator.Configuration.ReportGenerator.GeneratorReport(reportSpecs);

            var aggResults = new AggregationResults<T1> { Resolution = reportSpecs.Resolution };

            if (aggregationResults == null)
                return aggResults;

            foreach (var aggregationResult in aggregationResults.AggregationResults)
            {
                var aggResult = new AggregationResult<T1>();
                aggResult.MesurementTimeUtc = aggregationResult.MesurementTimeUtc;


                aggResult.Occurrence = aggregationResult.Occurence;
                aggResult.Total = aggregationResult.Total;



                foreach (var mV in aggregationResult.MeasurementValues)
                {
                    var type = aggResult.Tracker.GetType();


                    var property = type.GetProperty(mV.PropertyName);
                    if (property != null)
                    {
                        var convertedValue = Convert.ChangeType(mV.Value, property.PropertyType);
                        property.SetValue(aggResult.Tracker, convertedValue);
                    }

                }
                aggResults.Results.Add(aggResult);
            }

            return aggResults;
        }

    }
}




