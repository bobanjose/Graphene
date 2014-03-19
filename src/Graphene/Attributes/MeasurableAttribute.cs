using System;

namespace Graphene.Attributes
{
    public class MeasurableAttribute : Attribute
    {
        public MeasurableAttribute(string name = "", string description = "")
        {
            Name = name;
            Description = description;
        }

        public string Name { get; set; }
        public string Description { get; set; }
    }
}