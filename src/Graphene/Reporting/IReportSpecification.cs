using System;
using System.Collections.Generic;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    public interface IReportSpecification
    {
        IEnumerable<IFilterConditions> FilterCombinations { get; }

        IEnumerable<string> Counters { get; }

        DateTime FromDateUtc { get; }

        DateTime ToDateUtc { get; }

        string TrackerTypeName { get; }

    }
    public interface IFilterConditions
    {
        IEnumerable<string> Filters { get; }
    }

    internal class FilterConditions : IFilterConditions
    {
        private List<string> _filters;

        public FilterConditions(object filter)
        {
           _filters =  filter.GetPropertyNameValueList();

        }


        public IEnumerable<string> Filters
        {
            get { return _filters; }
        }
    }

}