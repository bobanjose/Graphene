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

        private readonly DateTime _fromDateUtc;
        private readonly ReportResolution _resolution;
        private readonly DateTime _toDateUtc;
        private IEnumerable<IMeasurement> _counters;
        private IEnumerable<IFilterConditions> _filterCombinations;
        

        public ReportSpecification(DateTime fromDateUtc, DateTime toDateUtc, ReportResolution resolution)
            : this(fromDateUtc, toDateUtc, resolution, new TFilter[] {})
        {
        }


        public ReportSpecification(DateTime fromDateUtc, DateTime toDateUtc, ReportResolution resolution,
            params TFilter[] filters)
        {
            _fromDateUtc = fromDateUtc;
            _toDateUtc = toDateUtc;
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

        public DateTime FromDateUtc
        {
            get { return _fromDateUtc; }
        }


        public DateTime ToDateUtc
        {
            get { return _toDateUtc; }
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

        private readonly DateTime _fromDateUtc;

        private readonly ReportResolution _resolution;

        private readonly DateTime _toDateUtc;
        private IEnumerable<Measurement> _counters;
        private IEnumerable<IFilterConditions> _filterCombinations;


        public ReportSpecification(IEnumerable<Type> trackerType, DateTime fromDateUtc, DateTime toDateUtc,
            ReportResolution resolution, params object[] filters) : this( trackerType,filters,fromDateUtc, toDateUtc, resolution )
        {
            
        }

        public ReportSpecification(IEnumerable<Type> trackerType, IEnumerable<object> filters, DateTime fromDateUtc,
            DateTime toDateUtc, ReportResolution resolution)
        {
            if(trackerType == null || !trackerType.Any())
                throw  new ArgumentException("You must provide at least one Tracker Type to measure!");

            _fromDateUtc = fromDateUtc;
            _toDateUtc = toDateUtc;
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


        public DateTime FromDateUtc
        {
            get { return _fromDateUtc; }
        }

        public DateTime ToDateUtc
        {
            get { return _toDateUtc; }
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