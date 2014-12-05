// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Graphene.Tracking
{
    internal class Counter
    {
        private long _occurance;
        private long _total;
        private bool _occuranceOveridden = false;

        private object syncLock = new object();

        internal Counter()
        {
            SearchTags = new List<string>();
            NamedMetrics = new ConcurrentDictionary<string, long>();
            KeyFilter = string.Empty;
        }

        internal ConcurrentDictionary<string, long> NamedMetrics { private set; get; }

        internal long Occurrence
        {
            get { return _occurance; }
        }

        internal long Total
        {
            get { return _total; }
        }

        internal List<string> SearchTags { get; set; }
        internal string KeyFilter { get; set; }

        internal void Increment(long by, string metricName)
        {
            if (metricName == "_Occurrence")
            {
                _occurance = by;
                _occuranceOveridden = true;
            }
            else
            {
                if (!_occuranceOveridden)
                    Interlocked.Increment(ref _occurance);
                if (metricName == null || metricName == "_Total")
                    Interlocked.Add(ref _total, by);
                else
                {
                    NamedMetrics.AddOrUpdate(metricName, by, (i, t) => t + by);
                }
            }
        }
    }
}