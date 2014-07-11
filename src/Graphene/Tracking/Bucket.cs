// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Graphene.Util;

namespace Graphene.Tracking
{
    internal class Bucket
    {
        private readonly ConcurrentDictionary<string, Counter> _counters;
        private readonly DateTime _expiresAfter;
        private readonly object _syncLock = new object();

        internal Bucket(int lifeTimeInSeconds, Resolution resolution, DateTime? mesaurementDate = null)
        {
            _expiresAfter = DateTime.Now.AddSeconds(lifeTimeInSeconds);
            _counters = new ConcurrentDictionary<string, Counter>();
            var mDate = mesaurementDate ?? DateTime.Now;
            
            switch (resolution)
            {
                case Resolution.FiveMinute:
                    TimeSlot = mDate.Round(TimeSpan.FromMinutes(5));
                    break;
                case Resolution.FifteenMinute:
                    TimeSlot = mDate.Round(TimeSpan.FromMinutes(15));
                    break;
                case Resolution.Hour:
                    TimeSlot = mDate.Round(TimeSpan.FromHours(1));
                    break;
                case Resolution.ThirtyMinute:
                    TimeSlot = mDate.Round(TimeSpan.FromMinutes(30));
                    break;
                case Resolution.Minute:
                    TimeSlot = mDate.Round(TimeSpan.FromMinutes(1));
                    break;
            }
        }

        internal ConcurrentDictionary<string, Counter> Counters
        {
            get { return _counters; }
        }

        internal DateTime TimeSlot { get; private set; }

        internal bool HasExpired
        {
            get { return DateTime.Now > _expiresAfter; }
        }

        internal void IncrementCounter(long by)
        {
            IncrementCounter(by, null, null);
        }

        internal void IncrementCounter(long by, string metricName)
        {
            IncrementCounter(by, metricName, null);
        }

        internal void IncrementCounter(long by, object filter)
        {
            IncrementCounter(by, null, filter);
        }

        internal void IncrementCounter(long by, string metricName, object filter)
        {
            string keyTag = string.Empty;
            List<String> propertyNv = null;

            if (filter != null)
            {
                propertyNv = filter.GetPropertyNameValueList();
                if (propertyNv.Count > 0)
                    keyTag = propertyNv.Aggregate((x, z) => string.Concat(x, ",", z));
            }

            Counter counter = null;
            if (_counters.ContainsKey(keyTag))
                counter = _counters[keyTag];
            else
            {
                lock (_syncLock)
                {
                    if (_counters.ContainsKey(keyTag))
                        counter = _counters[keyTag];
                    else
                    {
                        counter = new Counter();
                        if (propertyNv != null && propertyNv.Count > 0)
                        {
                            var searchTags = new List<string>();
                            searchTags.Add(propertyNv.Aggregate((x, z) => string.Concat(x, ",,", z)));
                            getAllSearchTags(propertyNv, searchTags);
                            counter.SearchTags = searchTags;
                            counter.KeyFilter = keyTag;
                        }
                        _counters.TryAdd(keyTag, counter);
                    }
                }
            }
            counter.Increment(by, metricName);
        }

        public void getAllSearchTags(List<string> filters, List<string> perms)
        {
            var s = new List<string>();
            var nvPairsDouble = new List<string>();
            nvPairsDouble.AddRange(filters);
            nvPairsDouble.AddRange(filters);

            for (int i = 0; i < filters.Count; i++)
            {
                for (int k = 0; k < filters.Count; k++)
                {
                    var tag = new List<string>();
                    int skipped = 0;
                    int picked = 0;
                    for (int j = 0; j < nvPairsDouble.Count; j++)
                    {
                        if (skipped > i + k)
                        {
                            tag.Add(nvPairsDouble[j]);
                            picked++;
                        }
                        else
                            skipped++;
                        if (picked + skipped >= filters.Count + k)
                            break;
                    }
                    tag.Sort();
                    if (tag.Count > 0)
                    {
                        string newTag = tag.Aggregate((x, z) => string.Concat(x, ",,", z));
                        if (!perms.Contains(newTag))
                        {
                            perms.Add(newTag);
                            if (tag.Count >= 3)
                                getAllSearchTags(tag, perms);
                        }
                    }
                }
            }
        }
    }
}