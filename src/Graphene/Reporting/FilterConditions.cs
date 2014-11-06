using System.Collections.Generic;
using System.Linq;
using Graphene.Util;

namespace Graphene.Reporting
{

    internal class FilterConditions<TFilter> : IFilterConditions
    {
        private readonly List<string> _filters = new List<string>();

        public FilterConditions(TFilter filter)
        {
            var propertyNameValueList = filter.GetPropertyNameValueListSorted();
            if (propertyNameValueList.Any())
                _filters.Add(propertyNameValueList.Aggregate((x, z) => string.Concat(x, ",,", z)));
        }

        public IEnumerable<string> Filters
        {
            get { return _filters; }
        }
    }

    internal class FilterConditions : IFilterConditions
    {
        private readonly List<string> _filters = new List<string>();

        public FilterConditions(object filter)
        {
            var propertyNameValueList = filter.GetPropertyNameValueListSorted();
            if (propertyNameValueList.Any())
                _filters.Add(propertyNameValueList.Aggregate((x, z) => string.Concat(x, ",,", z)));
        }

        public IEnumerable<string> Filters
        {
            get { return _filters; }
        }
    }
}