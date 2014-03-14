using System.Collections.Generic;
using Graphene.Util;

namespace Graphene.Reporting
{
    internal class FilterConditions : IFilterConditions
    {
        private readonly List<string> _filters;

        public FilterConditions(object filter)
        {
            _filters = filter.GetPropertyNameValueList();
        }


        public IEnumerable<string> Filters
        {
            get { return _filters; }
        }
    }
}