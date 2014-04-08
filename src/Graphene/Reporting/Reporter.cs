using System;
using System.Reflection;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public class Reporter<T, T1>
        where T : struct
        where T1 : ITrackable, new()
    {
        public static ReportResolution GetResolutionFromDates(DateTime fromUtc, DateTime toUtc)
        {
            var resolution = ReportResolution.Hour;

            double reportSpan = toUtc.Subtract(fromUtc).TotalDays;

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

        public static AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc,
            ReportSpecification<T, T1> reportSpecs) 
        
        {
            ITrackerReportResults trackerReportResults =
                Configurator.Configuration.ReportGenerator.BuildReport(reportSpecs);

            var aggResults = new AggregationResults<T1> {Resolution = reportSpecs.Resolution};

            if (trackerReportResults == null)
                return aggResults;

            foreach (IAggregationResult aggregationResult in trackerReportResults.AggregationResults)
            {
                var aggResult = new AggregationResult<T1>();
                aggResult.MesurementTimeUtc = aggregationResult.MesurementTimeUtc;


                aggResult.Occurrence = aggregationResult.Occurence;
                aggResult.Total = aggregationResult.Total;


                foreach (IMeasurementResult mV in aggregationResult.MeasurementValues)
                {
                    Type type = aggResult.Tracker.GetType();


                    PropertyInfo property = type.GetProperty(mV.PropertyName);
                    if (property != null)
                    {
                        object convertedValue = Convert.ChangeType(mV.Value, property.PropertyType);
                        property.SetValue(aggResult.Tracker, convertedValue);
                    }
                }
                aggResults.Results.Add(aggResult);
            }

            return aggResults;
        }
    }
}