using System.Collections.Generic;
using Graphene.Reporting;

namespace Graphene.API.Models
{
    public class JsonFilterCondition : IFilterConditions
    {
        public IEnumerable<string> Filters { get; set; }
    }
}