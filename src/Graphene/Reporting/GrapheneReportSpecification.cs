using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Graphene.Attributes;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public class GrapheneReportSpecification : IReportSpecification
    {
        private ReportResolution _resolution;
        
        private DateTime _toDateUtc;
        private DateTime _fromDateUtc;
        private IEnumerable<Measurement> _counters;
        private IEnumerable<IFilterConditions> _filterCombinations;

        private static readonly IEnumerable<string> _trackableProperties =  typeof (ITrackable).GetProperties().Select(x=> x.Name);


        public GrapheneReportSpecification(IEnumerable<Type> trackerType ,  DateTime fromDateUtc, DateTime toDateUtc, ReportResolution resolution,params object[] filters )
        {
            _fromDateUtc = fromDateUtc;
            _toDateUtc = toDateUtc;
            _resolution = resolution;
            buildFilterList(filters);
            buildListOfMeasurementsForTracker(trackerType);

        }

        public IEnumerable<IFilterConditions> FilterCombinations
        {
            get { return _filterCombinations; }
        }

        public IEnumerable<IMeasurement> Counters
        {
            get { return _counters; }
        }

        /*public IEnumerable<string> Counters
        {
            get { return _counters; }
        }*/

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
            
            _counters = trackables.SelectMany((x, y) => x.GetProperties()).
                Where(x => (!_trackableProperties.Contains(x.Name))
                           ||
                           (x.GetCustomAttribute(typeof (MeasurableAttribute)) != null))
                .Select(x => new Measurement(x)).ToList();
            TypeNames = _counters.Select(x => x.TrackerTypeName).Distinct();
        }

        private void buildFilterList(params object[] filters)
        {
            _filterCombinations = filters.Select(x => new FilterConditions(x)).ToList();
           
        }

    }
}