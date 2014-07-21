using System;

namespace Graphene.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class MeasurementAttribute : Attribute
    {
        public MeasurementAttribute(string name = "")
        {
            Name = name;
        }

        public string Name { get; private set; }

        public string DisplayName { get; set; }

        public string Description { get; set; }

        public string ToolTip { get; set; }

        public string Legend { get; set; }
    }
}