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
using System.Reflection;
using Graphene.Reporting;

namespace Graphene.Util
{
    public static class Extentions
    {

        public static IEnumerable<string> GetPropertyNameValueEnumeration(this object obj)
        {
            Type type = obj.GetType();
            var props = new List<PropertyInfo>(type.GetProperties());
            var nvList = new List<string>();
            foreach (PropertyInfo prop in props)
            {
                if (prop.PropertyType == typeof (String) || (Nullable.GetUnderlyingType(prop.PropertyType) != null))
                {
                    object propValue = prop.GetValue(obj, null);
                    if (propValue != null && !String.IsNullOrWhiteSpace(propValue.ToString()))
                    {
                        nvList.Add(string.Format("{0}::{1}", prop.Name.ToUpper(), propValue.ToString().ToUpper()));
                    }
                }
                else
                {
                    throw new Exception(
                        string.Format("All properties of the filter object of type {0}  have to be Nullable Types",
                            type.FullName));
                }
            }
            
            return nvList;
        }

        [Obsolete]
        public static List<string> GetPropertyNameValueList(this object obj)
        {
            Type type = obj.GetType();
            var props = new List<PropertyInfo>(type.GetProperties());
            var nvList = new List<string>();
            foreach (PropertyInfo prop in props.OrderBy(p=>p.Name))
            {
                if (prop.PropertyType == typeof(String) || (Nullable.GetUnderlyingType(prop.PropertyType) != null))
                {
                    object propValue = prop.GetValue(obj, null);
                    if (propValue != null && !String.IsNullOrWhiteSpace(propValue.ToString()))
                    {
                        nvList.Add(string.Format("{0}::{1}", prop.Name.ToUpper(), propValue.ToString().ToUpper()));
                    }
                }
                else
                {
                    throw new Exception(
                        string.Format("All properties of the filter object of type {0}  have to be Nullable Types",
                            type.FullName));
                }
            }

            return nvList;
        }

        public static List<string> GetPropertyNameValueListSorted(this object obj)
        {
            Type type = obj.GetType();
            var props = new List<PropertyInfo>(type.GetProperties());
            var nvList = new List<string>();
            foreach (PropertyInfo prop in props.OrderBy(p => p.Name))
            {
                if (prop.PropertyType == typeof(String) || (Nullable.GetUnderlyingType(prop.PropertyType) != null))
                {
                    object propValue = prop.GetValue(obj, null);
                    if (propValue != null && !String.IsNullOrWhiteSpace(propValue.ToString()))
                    {
                        nvList.Add(string.Format("{0}::{1}", prop.Name.ToUpper(), propValue.ToString().ToUpper()));
                    }
                }
                else
                {
                    throw new Exception(
                        string.Format("All properties of the filter object of type {0}  have to be Nullable Types",
                            type.FullName));
                }
            }

            return nvList;
        }

        public static TimeSpan Round(this TimeSpan time, TimeSpan roundingInterval, MidpointRounding roundingType)
        {
            return new TimeSpan(
                Convert.ToInt64(Math.Round(
                    time.Ticks/(decimal) roundingInterval.Ticks,
                    roundingType
                    ))*roundingInterval.Ticks
                );
        }

        public static TimeSpan Round(this TimeSpan time, TimeSpan roundingInterval)
        {
            return Round(time, roundingInterval, MidpointRounding.ToEven);
        }

        public static DateTime Round(this DateTime datetime, TimeSpan roundingInterval)
        {
            return new DateTime((datetime - DateTime.MinValue).Round(roundingInterval).Ticks);
        }
    }
}