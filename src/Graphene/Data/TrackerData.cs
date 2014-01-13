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

namespace Graphene.Data
{
    public class Measure
    {
        public long Occurrence { get; set; }
        public long Total { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
    }

    public class TrackerData
    {
        public TrackerData(string typeName)
        {
            TypeName = typeName;
        }

        public string _id { get {
            return String.Concat(TypeName,TimeSlot,KeyFilter);
        } }

        public string TypeName { get; set; }

        public string Name { get; set; }

        public DateTime TimeSlot { get; set; }

        public string KeyFilter { get; set; }

        public string[] SearchFilters { get; set; }

        public Measure Measurement { get; set; }
    }
}
