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
using System.Collections.Specialized;
using System.Threading;
using System.Reflection;

namespace Graphene.Tracking
{
    public class FilteredIncrement<T, T1> where T : struct where T1 : ITrackable
    {
        internal FilteredIncrement(Container<T1> container, T filter)
        {
            Container = container;
            Filter = filter;
        }
        internal FilteredIncrement()
        {
        }
        Container<T1> Container { get; set; }

        T Filter{ get; set; }       

        public void IncrementTracker(long by)
        {
            try
            {
                if (Container != null)
                    Container.IncrementTracker(by, Filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        } 
    }

    public static class Extentions
    {
        public static List<string> GetPropertyNameValueList(this object obj)
        {
            var type = obj.GetType();
            var props = new List<PropertyInfo>(type.GetProperties());
            var nvList = new List<string>();
            foreach (PropertyInfo prop in props)
            {
                if (prop.PropertyType == typeof(String) || (Nullable.GetUnderlyingType(prop.PropertyType) != null))
                {
                    var propValue = prop.GetValue(obj, null);
                    if (propValue != null)
                    {
                        nvList.Add(string.Format("{0}::{1}", prop.Name.ToUpper(), propValue.ToString().ToUpper()));
                    }
                }
                else
                {
                    throw new Exception("All properties have to be Nullable Types");
                }
            }
            nvList.Sort();
            return nvList;
        }

        public static TimeSpan Round(this TimeSpan time, TimeSpan roundingInterval, MidpointRounding roundingType)
        {
            return new TimeSpan(
                Convert.ToInt64(Math.Round(
                    time.Ticks / (decimal)roundingInterval.Ticks,
                    roundingType
                )) * roundingInterval.Ticks
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

    public abstract class ContainerBase
    {
        internal abstract IEnumerable<Data.TrackerData> GetTrackerData(bool flushAll);
    }

    public class Container<T1> : ContainerBase where T1 : ITrackable
    {
        private static List<Container<T1>> _trackers = new List<Container<T1>>();
        private static object _trackerLock = new object();

        private readonly Type _trackerType;
        private ITrackable _tracker;
        private object _syncLock = new object();
        private Queue<Bucket> _queuedBucket = new Queue<Bucket>();
        private Bucket _currentBucket;
        private long _bucketLifeInTicks;

        internal CancellationToken CancellationToken { get; set; }

        private readonly int _updateIntervalInSeconds = 180;

        internal Container()
        {
            _trackerType = typeof(T1);
            _tracker = (ITrackable)Activator.CreateInstance(_trackerType);            
        }

        public override bool Equals(object obj)
        {
            return _trackerType == typeof(T1);
        }

        public override int GetHashCode()
        {
            return _trackerType.GetHashCode();
        }

        internal override IEnumerable<Data.TrackerData> GetTrackerData(bool flushAll)
        {
            getCurrentBucket(flushAll);
            while (_queuedBucket.Count > 0)
            {
                var bucket = _queuedBucket.Dequeue();
                foreach (var counter in bucket.Counters.Values)
                {
                    yield return new Data.TrackerData((typeof(T1)).Name)
                    {
                        KeyFilter = counter.KeyFilter,
                        Name = _tracker.Name,
                        SearchFilters = counter.SearchTags.ToArray(),
                        TimeSlot = bucket.TimeSlot,
                        Measurement = new Data.Measure
                        {
                            Min = counter.MinValue,
                            Max = counter.MaxValue,
                            Occurance = counter.Occurance,
                            Total = counter.Total
                        }
                    };
                }
            }
        }

        private static Container<T1> getTracker()
        {
            var t = new Container<T1>();
            lock (_trackerLock)
            {
                t = _trackers.Where(t1 => t1.Equals(t)).FirstOrDefault();
                if (t == null)
                {
                    t = new Container<T1>();
                    _trackers.Add(t);
                    Publishing.Publisher.Register(t);
                }
            }
            return t;
        }

        private static Bucket getBucket()
        {
            var t = getTracker();
            return t.getCurrentBucket(false);
        }

        private Bucket getCurrentBucket(bool flushAll)
        {
            if (_currentBucket == null || _currentBucket.HasExpired || flushAll)
            {
                lock (_syncLock)
                {
                    if (_currentBucket == null || _currentBucket.HasExpired || flushAll)
                    {
                        if (_currentBucket != null)
                            _queuedBucket.Enqueue(_currentBucket);
                        _currentBucket = new Bucket(_updateIntervalInSeconds, _tracker.MinResolution);
                    }
                }
            }

            return _currentBucket;
        }

        public static FilteredIncrement<T, T1> Where<T>(T filter) where T : struct
        {
            try
            {
                return new FilteredIncrement<T, T1>(getTracker(), filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
                return new FilteredIncrement<T, T1>();
            }
        }

        public static void IncrementTracker(long by)
        {
            try
            {
                var bucket = getBucket();
                bucket.IncrementCounter(by);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }

        internal void IncrementTracker(long by, object filter)
        {
            var bucket = getBucket();
            bucket.IncrementCounter(by, filter);
        }


        private static IEnumerable<string> getPermutations(List<string> items)
        {
            var s = new List<string>();
            var cs = new List<string>();
            cs.AddRange(items);
            cs.AddRange(items);

            for (var i = 0; i < items.Count; i++)
            {
                for (var k = 0; k < items.Count; k++)
                {
                    var t = "";
                    var skipped = 0;
                    var picked = 0;
                    for (var j = 0; j < cs.Count; j++)
                    {
                        //if (j != numToSkip+i)
                        if (skipped > i + k)
                        {
                            t = string.Concat(t, cs[j]);
                            picked++;
                        }
                        else
                            skipped++;
                        if (picked + skipped >= items.Count + k)
                            break;
                    }
                    char[] c = t.ToCharArray();
                    Array.Sort(c);
                    Console.WriteLine(new String(c));
                }
            }
            return null;
        }
    }
}
