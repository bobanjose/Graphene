// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Graphene.Data;
using Graphene.Publishing;
using Graphene.Reporting;
using Graphene.Util;

namespace Graphene.Tracking
{
    public class AddNamedMetric<T1>
    {
        private readonly Bucket _bucket;
        private readonly object _filter;

        internal AddNamedMetric(Bucket bucket, object filter)
        {
            _bucket = bucket;
            _filter = filter;
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannot be null");
            if (by.IsRunning)
                by.Stop();
            return Increment(incAttr, by.ElapsedMilliseconds);
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, long by)
        {
            try
            {
                PropertyInfo pi = PropertyHelper<T1>.GetProperty(incAttr);
                _bucket.IncrementCounter(by, pi.Name, _filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return this;
        }
    }

    public class FilteredOperations<T, T1> where T : struct where T1 : ITrackable, new()
    {
        internal FilteredOperations(Container<T1> container, T filter)
        {
            Container = container;
            Filter = filter;
        }

        internal FilteredOperations()
        {
        }

        private Container<T1> Container { get; set; }

        private T Filter { get; set; }

        public void IncrementBy(Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannot be null");
            if (by.IsRunning)
                by.Stop();

            IncrementBy(by.ElapsedMilliseconds);
        }

        public AddNamedMetric<T1> Increment(Expression<Func<T1, long>> incAttr, Stopwatch by)
        {
            if (by == null)
                throw new ArgumentNullException("by cannot be null");
            if (by.IsRunning)
                by.Stop();

            return Increment(incAttr, by.ElapsedMilliseconds);
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
            return Report(fromUtc, toUtc, resolution, null);
        }

        public AggregationResults<T1> Report(DateTime fromUtc, DateTime toUtc, ReportResolution resolution, TimeSpan? offsetInterval)
        {
            var reportSpecs = new ReportSpecification<T, T1>(fromUtc, toUtc, resolution, Filter);

            if (offsetInterval != null)
                reportSpecs.OffsetFromUtcInterval = offsetInterval.GetValueOrDefault();

            return Reporter<T, T1>.Report(fromUtc, toUtc, reportSpecs);
        }
    }

    public abstract class ContainerBase
    {
        internal abstract IEnumerable<TrackerData> GetTrackerData(bool flushAll, bool includePreAggregatedBuckets);
    }

    public class Container<T1> : ContainerBase where T1 : ITrackable, new()
    {
        private static readonly List<Container<T1>> _trackers = new List<Container<T1>>();
        private static ReaderWriterLockSlim _trackerLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
        private readonly Queue<Bucket> _queuedBucket = new Queue<Bucket>();
        private readonly object _syncLock = new object();
        private ITrackable _tracker;
        private Type _trackerType;
        private const int UPDATE_INTERVAL_IN_SECONDS = 180;        

        private Bucket _currentBucket;

        public Container()
        {
            _trackerType = typeof (T1);
            _tracker = new T1();
        }

        internal CancellationToken CancellationToken { get; set; }

        public override bool Equals(object obj)
        {
            return _trackerType == typeof (T1);
        }

        public override int GetHashCode()
        {
            return _trackerType.GetHashCode();
        }

        internal override IEnumerable<TrackerData> GetTrackerData(bool flushAll, bool includePreAggregatedBuckets)
        {
            getCurrentBucket(flushAll);
            var trackerData = new List<TrackerData>();
            while (_queuedBucket.Count > 0)
            {
                Bucket bucket = _queuedBucket.Dequeue();
                foreach (var counter in bucket.Counters.Values)
                {
                    trackerData.Add(new TrackerData((typeof (T1)).FullName, _tracker.MinResolution)
                    {
                        KeyFilter = counter.KeyFilter,
                        Name = _tracker.Name,
                        SearchFilters = counter.SearchTags.ToArray(),
                        TimeSlot = bucket.TimeSlot,
                        Measurement = new Measure
                        {
                            _Occurrence = counter.Occurrence,
                            _Total = counter.Total,
                            NamedMetrics = counter.NamedMetrics,
                            CoveredResolutions = bucket.CoveredResolutions,
                            BucketResolution = bucket.BucketResolution
                        }
                    });
                }
                if (includePreAggregatedBuckets)
                {
                    foreach (var lowRezBucket in bucket.LowResolutionBuckets)
                    {
                        foreach (var counter in lowRezBucket.Counters.Values)
                        {
                            trackerData.Add(new TrackerData((typeof (T1)).FullName, _tracker.MinResolution)
                            {
                                KeyFilter = counter.KeyFilter,
                                Name = _tracker.Name,
                                SearchFilters = counter.SearchTags.ToArray(),
                                TimeSlot = lowRezBucket.TimeSlot,
                                Measurement = new Measure
                                {
                                    _Occurrence = counter.Occurrence,
                                    _Total = counter.Total,
                                    NamedMetrics = counter.NamedMetrics,
                                    CoveredResolutions = lowRezBucket.CoveredResolutions,
                                    BucketResolution = lowRezBucket.BucketResolution
                                }
                            });
                        }
                    }
                }
            }
            return trackerData;
        }

        private static Container<T1> getTrackerContainer(Container<T1> trackerContainer = null)
        {
            Container<T1> t = null;
            _trackerLock.EnterReadLock();
            try
            {
                t = _trackers.FirstOrDefault(t1 => t1.Equals(t));
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
                    t = _trackers.FirstOrDefault(t1 => t1.Equals(t));
                    if (t == null)
                    {
                        t = trackerContainer ?? new Container<T1>();
                        _trackers.Add(t);
                        Publisher.Register(t);
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
            Container<T1> t = getTrackerContainer();
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
                        _currentBucket = new Bucket(UPDATE_INTERVAL_IN_SECONDS, _tracker.MinResolution);
                    }
                }
            }

            return _currentBucket;
        }

        //Need another bucket for which, caller provides time
        //Cannot use _currentBucket because once time is set, that time will be used for all next calls untill bucket expires
        
        private Bucket getTimedBucket(DateTime measurementDate, Resolution minResolution)
        {
            Bucket timedBucket = null;
            try
            {
                timedBucket = _queuedBucket.FirstOrDefault(x => x.TimeSlot == measurementDate && !x.HasExpired);
            }
            catch (InvalidOperationException)
            {
                //Collection was modified after the enumerator was instantiated.
                //In this case, we will create a new bucket
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }

            if (timedBucket == null)
            {
                timedBucket = new Bucket(UPDATE_INTERVAL_IN_SECONDS, minResolution, measurementDate);
                //timedBucket.setTimeSlot(measurementDate);
                _queuedBucket.Enqueue(timedBucket);
            }
            return timedBucket;
        }

        public static FilteredOperations<T, T1> Where<T>(T filter) where T : struct
        {
            try
            {
                return new FilteredOperations<T, T1>(getTrackerContainer(), filter);
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
                throw new ArgumentNullException("by cannot be null");
            if (by.IsRunning)
                by.Stop();

            IncrementBy(by.ElapsedMilliseconds);
        }

        public static void IncrementBy(long by)
        {
            try
            {
                Bucket bucket = GetBucket();
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

        internal void Increment(ITrackable tracker, Resolution minResolution, DateTime timeSlot, string propertyName, long by, object filter)
        {
            try
            {
                _trackerType = typeof(T1);
                _tracker = tracker;
                var trackerContainer = getTrackerContainer(this);

                var bucket = new Bucket(0, minResolution, timeSlot);
                trackerContainer._queuedBucket.Enqueue(bucket);
                bucket.IncrementCounter(by, propertyName, filter);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }

        internal void Increment(Expression<Func<T1, long>> incAttr, long by, object filter)
        {
            try
            {
                PropertyInfo pi = PropertyHelper<T1>.GetProperty(incAttr);
                Bucket bucket = GetBucket();
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
                PropertyInfo pi = PropertyHelper<T1>.GetProperty(incAttr);
                bucket = GetBucket();
                bucket.IncrementCounter(by, pi.Name);
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return new AddNamedMetric<T1>(bucket, null);
        }        

        /// <summary>
        /// Sets the measurement date on the bucket
        /// </summary>
        /// <param name="measurementDate">Measurement date</param>
        /// <returns></returns>
        public static AddNamedMetric<T1> SetMeasurementDate(DateTime measurementDate)
        {            
            Bucket bucket = null;
            try
            {
                var t = getTrackerContainer();
                //var roundedMeasurementDate = roundDateWithResolution(measurementDate, t._tracker.MinResolution);
                bucket = t.getTimedBucket(measurementDate, t._tracker.MinResolution);               
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
            return new AddNamedMetric<T1>(bucket, null);
        }

        internal void IncrementBy(long by, object filter)
        {
            Bucket bucket = GetBucket();
            bucket.IncrementCounter(by, filter);
        }


        private static IEnumerable<string> getPermutations(List<string> items)
        {
            var s = new List<string>();
            var cs = new List<string>();
            cs.AddRange(items);
            cs.AddRange(items);

            for (int i = 0; i < items.Count; i++)
            {
                for (int k = 0; k < items.Count; k++)
                {
                    string t = "";
                    int skipped = 0;
                    int picked = 0;
                    for (int j = 0; j < cs.Count; j++)
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

        private static DateTime roundDateWithResolution(DateTime date, Resolution resolution)
        {
            var returnDate = new DateTime();
            switch (resolution)
            {
                case Resolution.FiveMinute:
                    returnDate = date.Round(TimeSpan.FromMinutes(5));
                    break;
                case Resolution.FifteenMinute:
                    returnDate = date.Round(TimeSpan.FromMinutes(15));
                    break;
                case Resolution.Hour:
                    returnDate = date.Round(TimeSpan.FromHours(1));
                    break;
                case Resolution.ThirtyMinute:
                    returnDate = date.Round(TimeSpan.FromMinutes(30));
                    break;
                case Resolution.Minute:
                    returnDate = date.Round(TimeSpan.FromMinutes(1));
                    break;
            }
            return returnDate;
        }
    }

    public struct EmptyFilter
    {
    }
}