using System.Reflection;
using Graphene.Attributes;

namespace Graphene.Reporting
{
    internal class Measurement : IMeasurement
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="T:System.Object"/> class.
        /// </summary>
        internal Measurement(PropertyInfo property)
        {
            var attribute = property.GetCustomAttribute(typeof (MeasurableAttribute)) as MeasurableAttribute;
            if (attribute != null)
            {
                DisplayName = attribute.Name;
                Description = attribute.Description;
            }
            else
            {
                DisplayName = property.Name;
                Description = property.Name;
                
            }
            PropertyName = property.Name;
            TrackerTypeName = property.DeclaringType.FullName;

        }

        public string PropertyName { get; private set; }

        public string TrackerTypeName { get; private set; }

        public string DisplayName { get; private set; }

        public string Description { get; private set; }

        public string FullyQualifiedField
        {
            get { return string.Format("{0}.{1}",TrackerTypeName,PropertyName ); }
        }
    }
}