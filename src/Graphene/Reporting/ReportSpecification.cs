using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using Graphene.Attributes;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public class ReportSpecification<TTrackable> : IReportSpecification
        where TTrackable : ITrackable
    {

        private static readonly IEnumerable<string> _trackableProperties = typeof (ITrackable).GetProperties().Select(x=> x.Name);
        private List<IFilterConditions> _filters;
        private List<string> _counters;
        private DateTime _fromDateUtc;
        private DateTime _toDateUtc;
        private string _trackerTypeName;

        public ReportSpecification(DateTime fromDateUtc, DateTime toUtcDate, params object[] filters)
        {
            _fromDateUtc = fromDateUtc;
            _toDateUtc = toUtcDate;
            _trackerTypeName = typeof (TTrackable).FullName;
            _filters = new List<IFilterConditions>(filters.Count());
            buildFilterList(filters);
            buildListOfCountersForTracker();
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

        private void buildListOfCountersForTracker()
        {
            MeasurableAttribute attribute = new MeasurableAttribute();
            
            _counters = (from counter in typeof (TTrackable).GetProperties()
                         where (counter.GetCustomAttribute(typeof(MeasurableAttribute)) != null)
                select (counter.Name)).ToList();
        }

        private void buildFilterList(params object[] filters)
        {
            foreach (var filter in filters)
            {
                _filters.Add(new FilterConditions(filter));
            }
        }
    }
}