using System;

namespace Graphene.Attributes
{
    public class MeasurableAttribute : Attribute
    {
        public string Name { get; set; }
        public string Description { get; set; }

        public MeasurableAttribute(string name = "", string description = "")
        {
            Name = name;
            Description = description;
        }
    }
}