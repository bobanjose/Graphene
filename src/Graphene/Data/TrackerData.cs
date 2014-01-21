// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Graphene.Data
{
    public class Measure
    {
        public long _Occurrence { get; internal set; }
        public long _Total { get; internal set; }
        public long _Min { get; internal set; }
        public long _Max { get; internal set; }
        public ConcurrentDictionary<string, long> NamedMetrics { get; internal set; } 
    }

    public class TrackerData
    {
        public TrackerData(string typeName)
        {
            TypeName = typeName;
            KeyFilter = string.Empty;
        }

        public string _id { get {
            return String.Concat(TypeName,TimeSlot.ToUniversalTime(),KeyFilter).Replace(" ","");
        } }

        public string TypeName { get; internal set; }

        public string Name { get; internal set; }

        public DateTime TimeSlot { get; internal set; }

        public string KeyFilter { get; internal set; }

        public string[] SearchFilters { get; internal set; }

        public Measure Measurement { get; internal set; }
    }
}
