namespace Graphene.Reporting
{
    public interface IReportGenerator
    {
        /*IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification);*/

        ITrackerReportResults BuildReport(IReportSpecification specification);
    }
}