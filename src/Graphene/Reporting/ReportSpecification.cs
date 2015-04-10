using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Graphene.Attributes;
using Graphene.Tracking;
using Graphene.Util;

namespace Graphene.Reporting
{

    #region Generic Implementation

    public class ReportSpecification<TFilter, TTracker> : IReportSpecification
        where
            TFilter : struct
        where
            TTracker : ITrackable, new()
    {
        private static readonly IEnumerable<string> _trackableProperties =
            typeof (ITrackable).GetProperties().Select(x => x.Name);

        private readonly DateTime _fromDateTime;
        private readonly ReportResolution _resolution;
        private readonly DateTime _toDateTime;
        private IEnumerable<IMeasurement> _counters;
        private IEnumerable<IFilterConditions> _filterCombinations;
        

        public ReportSpecification(DateTime fromDateTime, DateTime toDateTime, ReportResolution resolution)
            : this(fromDateTime, toDateTime, resolution, new TFilter[] {})
        {
        }


        public ReportSpecification(DateTime fromDateTime, DateTime toDateTime, ReportResolution resolution,
            params TFilter[] filters)
        {
            _fromDateTime = fromDateTime;
            _toDateTime = toDateTime;
            _resolution = resolution;
            buildFilterList(filters);
            buildListOfMeasurementsForTracker(new[] {typeof (TTracker)});
            TypeNames = new[] {typeof (TTracker).FullName};
        }


        public TimeSpan OffsetFromUtcInterval { get; set; }

        public IEnumerable<IFilterConditions> FilterCombinations
        {
            get { return _filterCombinations; }
        }

        public IEnumerable<IMeasurement> Counters
        {
            get { return _counters; }
        }

        public DateTime FromDateTime
        {
            get { return _fromDateTime; }
        }


        public DateTime ToDateTime
        {
            get { return _toDateTime; }
        }

        public IEnumerable<string> TypeNames { get; private set; }


        public ReportResolution Resolution
        {
            get { return _resolution; }
        }

        private void buildFilterList(params TFilter[] filters)
        {
            _filterCombinations = filters.Select(x => new FilterConditions<TFilter>(x)).Where(fc => fc.Filters.Any()).ToList();
        }

        private void buildListOfMeasurementsForTracker(IEnumerable<Type> trackables)
        {
            _counters = trackables.SelectMany((x, y) => x.GetProperties()).
                Where(x => (!_trackableProperties.Contains(x.Name))
                           ||
                           (x.GetCustomAttribute(typeof (MeasurementAttribute)) != null))
                .Select(x => new Measurement(x)).ToList();
        }
    }

    #endregion Generic Implementation

    #region looselytyped implementation
    public class ReportSpecification : IReportSpecification
    {
        private static readonly IEnumerable<string> _trackableProperties =
            typeof(ITrackable).GetProperties().Select(x => x.Name);

        private readonly DateTime _fromDateTime;

        private readonly ReportResolution _resolution;

        private readonly DateTime _toDateTime;
        private IEnumerable<Measurement> _counters;
        private IEnumerable<IFilterConditions> _filterCombinations;


        public ReportSpecification(IEnumerable<Type> trackerType, DateTime fromDateUtc, DateTime toDateUtc,
            ReportResolution resolution, params object[] filters) : this( trackerType,filters,fromDateUtc, toDateUtc, resolution )
        {
            
        }

        public ReportSpecification(IEnumerable<Type> trackerType, IEnumerable<object> filters, DateTime fromDateTime,
            DateTime toDateTime, ReportResolution resolution)
        {
            if(trackerType == null || !trackerType.Any())
                throw  new ArgumentException("You must provide at least one Tracker Type to measure!");

            _fromDateTime = fromDateTime;
            _toDateTime = toDateTime;
            _resolution = resolution;
            buildFilterList(filters);
            buildListOfMeasurementsForTracker(trackerType);

        }

        public TimeSpan OffsetFromUtcInterval { get; set; }

        public IEnumerable<IFilterConditions> FilterCombinations
        {
            get { return _filterCombinations; }
        }

        public IEnumerable<IMeasurement> Counters
        {
            get { return _counters; }
        }


        public DateTime FromDateTime
        {
            get { return _fromDateTime; }
        }

        public DateTime ToDateTime
        {
            get { return _toDateTime; }
        }

        public IEnumerable<string> TypeNames { get; private set; }


        public ReportResolution Resolution
        {
            get { return _resolution; }
        }

        private void buildListOfMeasurementsForTracker(IEnumerable<Type> trackables)
        {
            _counters = trackables.Distinct().SelectMany((x, y) => x.GetProperties()).
                Where(x => (!_trackableProperties.Contains(x.Name))
                           ||
                           (x.GetCustomAttribute(typeof(MeasurementAttribute)) != null))
                .Select(x => new Measurement(x)).ToList();
            TypeNames = _counters.Select(x => x.TrackerTypeName).Distinct();
        }

        private void buildFilterList(IEnumerable<object> filters)
        {
            _filterCombinations = filters.Select(x => new FilterConditions(x)).Where(fc => fc.Filters.Any()).ToList();
        }
    }
    #endregion
}