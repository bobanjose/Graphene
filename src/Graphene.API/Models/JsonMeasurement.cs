using Graphene.Reporting;

namespace Graphene.API.Models
{
    public class JsonMeasurement : IMeasurement
    {
        public string PropertyName { get; set; }
        public string TrackerTypeName { get; set; }
        public string DisplayName { get; set; }
        public string Description { get; set; }
        public string FullyQualifiedPropertyName { get; set; }
    }
}