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

namespace Graphene.Tracking
{
    internal class Counter
    {
        internal Counter()
        {
            SearchTags = new List<string>();
        }

        object syncLock = new object();

        internal long Occurance { get; private set; }

        internal long Total { get; private set; }

        internal long MinValue { get; private set; }

        internal long MaxValue { get; private set; }

        internal void Increment(long by)
        {
            lock (syncLock)
            {
                Occurance++;
                Total = Total + by;
                if (MinValue > by)
                    MinValue = by;
                if (MaxValue < by)
                    MaxValue = by;
            }
        }

        internal List<string> SearchTags { get; set; }
        internal string KeyFilter { get; set; }
    }
}
