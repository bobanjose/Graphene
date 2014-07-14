// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.Linq.Expressions;
using Graphene.Util;

namespace Graphene.Attributes
{
     public static class AttributeHelperExtentions
    {
        public static TValue GetAttributeValue<TAttribute, TValue>(this Type type, Func<TAttribute, TValue> valueOf) where TAttribute : Attribute
        {
            var attribute = type.GetCustomAttributes(typeof(TAttribute), true).OfType<TAttribute>().FirstOrDefault();

            return attribute != null ? valueOf(attribute) : default(TValue);
        }

        public static TValue GetAttributeValue<TTrackerClass, TAttribute, TValue>(this Type type, Expression<Func<TTrackerClass, long>> propSelector, Func<TAttribute, TValue> valueOf)
            where TAttribute : Attribute
            where TTrackerClass : class
        {
            var propertyInfo = PropertyHelper<TTrackerClass>.GetProperty(propSelector);

            var attribute = propertyInfo.GetCustomAttributes(typeof(TAttribute), true).OfType<TAttribute>().FirstOrDefault();

            return attribute != null ? valueOf(attribute) : default(TValue);
        }
    }
}
