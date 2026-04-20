using System;

namespace Graphene.API
{
    public static class WebApiConfig
    {
        // Placeholder for legacy Web API startup. In ASP.NET Core this is handled in Program.cs.
        public static void Register(object config = null)
        {
            // no-op
        }

        private static TimeSpan getDayTotalTimeZoneOffset(int offsetHours)
        {
            TimeSpan dayTotalTzOffset;
            if (offsetHours >= -11 && offsetHours <= 14)
            {
                dayTotalTzOffset = new TimeSpan(offsetHours, 0, 0);
            }
            else
            {
                dayTotalTzOffset = new TimeSpan(0, 0, 0);
            }
            return dayTotalTzOffset;
        }
    }

    // legacy converters removed - kept minimal stubs to satisfy any references
    public class JsonConverterReportSpecificationConverter { }
    public class JsonConverterFilterCombinationsConverter { }
    public class JsonConverterMeasurementConverter { }
    public static class IntExtensions
    {
        public static int TryParse(this int number, string stringNumber, int defaultNumber)
        {
            var parseNumber = 0;
            return int.TryParse(stringNumber, out parseNumber) ? parseNumber : defaultNumber;
        }
    }
}