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
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Threading;
using System.Reflection;
using System.Diagnostics;

using Graphene.Util;
using Graphene.Reporting;

namespace Graphene.Tracking
{
    public class AddNamedMetric<T1>
    {
        Bucket _bucket;
        object _filter;

        internal AddNamedMetric(Bucket bucket, object filter)
        {
            _bucket = bucket;
            _filter = filter;
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannong be null");
            if (by.IsRunning)
                by.Stop();
            return this.Increment(incAttr, by.ElapsedMilliseconds);
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, long by)
        {
            Bucket bucket;
            try
            {
                var pi = PropertyHelper<T1>.GetProperty(incAttr);
                _bucket.IncrementCounter(by, pi.Name, _filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return this;
        }
    }

    public class FilteredOperations<T, T1> where T : struct where T1 : ITrackable
    {
        internal FilteredOperations(Container<T1> container, T filter)
        {
            Container = container;
            Filter = filter;
        }

        internal FilteredOperations()
        {
        }

        Container<T1> Container { get; set; }

        T Filter{ get; set; }

        public void IncrementBy(Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannong be null");
            if (by.IsRunning)
                by.Stop();

            this.IncrementBy(by.ElapsedMilliseconds);
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannong be null");
            if (by.IsRunning)
                by.Stop();

            return this.Increment(incAttr, by.ElapsedMilliseconds);
        }

        public void IncrementBy(long by)
        {
            try
            {
                if (Container != null)
                    Container.IncrementBy(by, Filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, long by)
        {
            try
            {
                if (Container != null)
                    Container.Increment(incAttr, by, Filter);
                return new AddNamedMetric<T1>(Container<T1>.GetBucket(), Filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return null;
        }

        public AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc)
        {
            return Report(fromUtc, toUtc, Reporter<T, T1>.GetResolutionFromDates(fromUtc, toUtc));
        }

        public AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc, ReportResolution resolution)
        {
            var reportSpecs = new ReportSpecification<T, T1>(fromUtc, toUtc, resolution, Filter);

            return Reporter<T, T1>.Report(fromUtc, toUtc, reportSpecs);
        }
    }
    
    public abstract class ContainerBase
    {
        internal abstract IEnumerable<Data.TrackerData> GetTrackerData(bool flushAll);
    }

    public class Container<T1> : ContainerBase where T1 : ITrackable
    {
        private static List<Container<T1>> _trackers = new List<Container<T1>>();
        private static ReaderWriterLockSlim _trackerLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

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
                    yield return new Data.TrackerData((typeof(T1)).FullName)
                    {
                        KeyFilter = counter.KeyFilter,
                        Name = _tracker.Name,
                        SearchFilters = counter.SearchTags.ToArray(),
                        TimeSlot = bucket.TimeSlot,
                        Measurement = new Data.Measure
                        {
                            _Occurrence = counter.Occurrence,
                            _Total = counter.Total,
                            NamedMetrics = counter.NamedMetrics
                        }                        
                    };
                }
            }
        }

        private static Container<T1> getTracker()
        {
            var t = new Container<T1>();
            _trackerLock.EnterReadLock();
            try
            {
                t = _trackers.Where(t1 => t1.Equals(t)).FirstOrDefault();
            }
            finally
            {
                _trackerLock.ExitReadLock();
            }
            if (t == null)
            {
                _trackerLock.EnterWriteLock();
                try
                {
                    t = _trackers.Where(t1 => t1.Equals(t)).FirstOrDefault();
                    if (t == null)
                    {
                        t = new Container<T1>();
                        _trackers.Add(t);
                        Publishing.Publisher.Register(t);
                    }
                }
                finally
                {
                    _trackerLock.ExitWriteLock();
                }
            }
            
            return t;
        }

        internal static Bucket GetBucket()
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

        public static FilteredOperations<T, T1> Where<T>(T filter) where T : struct
        {
            try
            {
                return new FilteredOperations<T, T1>(getTracker(), filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
                return new FilteredOperations<T, T1>();
            }
        }

        public static void IncrementBy(Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannong be null");
            if (by.IsRunning)
                by.Stop();

            IncrementBy(by.ElapsedMilliseconds);
        }

        public static void IncrementBy(long by)
        {
            try
            {
                var bucket = GetBucket();
                bucket.IncrementCounter(by);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }

        public static AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc)
        {
            return Report(fromUtc, toUtc, Reporter<EmptyFilter, T1>.GetResolutionFromDates(fromUtc, toUtc));
        }

        public static AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc, ReportResolution resolution)
        {
            var reportSpecs = new ReportSpecification<EmptyFilter, T1>(fromUtc, toUtc, resolution);
            return Reporter<EmptyFilter, T1>.Report(fromUtc, toUtc, reportSpecs);
        }

        internal void Increment(Expression<Func<T1, long>> incAttr, long by, object filter)
        {
            try
            {
                var pi = PropertyHelper<T1>.GetProperty(incAttr);
                var bucket = GetBucket();
                bucket.IncrementCounter(by, pi.Name, filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }        

        public static AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, long by)
        {
            Bucket bucket = null;
            try
            {
                var pi = PropertyHelper<T1>.GetProperty(incAttr);
                bucket = GetBucket();
                bucket.IncrementCounter(by, pi.Name);                  
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return new AddNamedMetric<T1>(bucket, null);
        }

        internal void IncrementBy(long by, object filter)
        {
            var bucket = GetBucket();
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

    public struct EmptyFilter
    {
    }
}
