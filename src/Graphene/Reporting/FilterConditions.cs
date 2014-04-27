using System.Collections.Generic;
using Graphene.Util;

namespace Graphene.Reporting
{
    internal class FilterConditions : IFilterConditions
    {
        private readonly IEnumerable<string> _filters;

        public FilterConditions(object filter)
        {
            _filters = filter.GetPropertyNameValueList();
        }

        public FilterConditions(IEnumerable<string> filters)
        {
            _filters = filters;

        }

        public IEnumerable<string> Filters
        {
            get { return _filters; }
        }
    }
}