using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Attributes
{
    public static class AttributeHelperExtentions
    {
        public static TValue GetAttributeValue<TAttribute, TValue>(this Type type, Func<TAttribute, TValue> valueOf) where TAttribute : Attribute
        {
            var attribute = type.GetCustomAttributes(typeof(TAttribute), true).OfType<TAttribute>().FirstOrDefault();

            return attribute != null ? valueOf(attribute) : default(TValue);
        }
    }
}
