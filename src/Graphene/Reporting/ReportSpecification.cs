using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public class ReportSpecification<TFilter, TTrackable> : IReportSpecification
        where TTrackable : ITrackable
    {

        private static readonly IEnumerable<string> _trackableProperties = typeof (ITrackable).GetProperties().Select(x=> x.Name);
        private List<IFilterConditions> _filters;
        private List<string> _counters;
        private DateTime _fromDateUtc;
        private DateTime _toDateUtc;
        private string _trackerTypeName;
        private ReportResolution _reportResolution;

        public ReportSpecification(DateTime fromDateUtc, DateTime toUtcDate, ReportResolution resolution, params TFilter[] filters)
        {
            _fromDateUtc = fromDateUtc;
            _toDateUtc = toUtcDate;
            _trackerTypeName = typeof (TTrackable).FullName;
            _filters = new List<IFilterConditions>(filters.Count());
            buildFilterList(filters);
            buildListOfCountersForTracker();
            _reportResolution = resolution;
        }


        public IEnumerable<IFilterConditions> FilterCombinations
        {
            get { return _filters; }
        }

        public IEnumerable<string> Counters
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

        public string TrackerTypeName
        {
            get { return _trackerTypeName; }
        }

        public ReportResolution Resolution
        {
            get { return _reportResolution; }
            internal set{ _reportResolution = value; }
        }

        private void buildListOfCountersForTracker()
        {            
            _counters = (from counter in typeof (TTrackable).GetProperties()
                where (! _trackableProperties.Contains(counter.Name))
                select (counter.Name)).ToList();
        }

        private void buildFilterList(params TFilter[] filters)
        {
            if (filters == null)
                return;
            foreach (var filter in filters)
            {
                _filters.Add(new FilterConditions<TFilter>(filter));
            }
        }
    }
}