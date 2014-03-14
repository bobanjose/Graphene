using Graphene.Reporting;

namespace Graphene.Mongo.Reporting
{
    internal static class ReportingExtensions
    {
        private const char namespaceDelimiter = '^';
        private const char propertyNameDelimiter = '_';

        internal static string FormatFieldName(this IMeasurement measurement)
        {
            return string.Format("{0}{1}{2}", measurement.TrackerTypeName.Replace('.', namespaceDelimiter),
                propertyNameDelimiter, measurement.PropertyName);
        }

        internal static string GetFullyQualifiedNameFromFormattedString(this string formattedString)
        {
            return formattedString.Replace(namespaceDelimiter, '.').Replace(propertyNameDelimiter, '.');
        }
    }
}