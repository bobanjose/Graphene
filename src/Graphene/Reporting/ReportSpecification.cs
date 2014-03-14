using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Graphene.Tracking;
using Graphene.Attributes;

namespace Graphene.Reporting
{

    #region Generic Implementation

    public class ReportSpecification<TFilter, TTracker> : IReportSpecification
        where
            TFilter : struct
        where
            TTracker : ITrackable
    {
        private static readonly IEnumerable<string> _trackableProperties =
            typeof (ITrackable).GetProperties().Select(x => x.Name);

        private IEnumerable<IFilterConditions> _filterCombinations;
        private IEnumerable<IMeasurement> _counters;
        private DateTime _fromDateUtc;
        private DateTime _toDateUtc;
        private string _trackerTypeName;
        private ReportResolution _resolution;

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
            buildListOfMeasurementsForTracker(new Type[] {typeof (TTracker)});
            this.TypeNames = new string[] {typeof (TTracker).FullName};
        }


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
            _filterCombinations = filters.Select(x => new FilterConditions(x)).ToList();
        }

        private void buildListOfMeasurementsForTracker(IEnumerable<Type> trackables)
        {
            _counters = trackables.SelectMany((x, y) => x.GetProperties()).
                Where(x => (!_trackableProperties.Contains(x.Name))
                           ||
                           (x.GetCustomAttribute(typeof (MeasurableAttribute)) != null))
                .Select(x => new Measurement(x)).ToList();
        }
    }

    #endregion Generic Implementation

    
}