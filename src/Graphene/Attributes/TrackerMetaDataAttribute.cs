using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Attributes
{
    [AttributeUsage(AttributeTargets.All)]
    public class TrackerMetaDataAttribute : Attribute
    {
        
        public TrackerMetaDataAttribute(string name)
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
