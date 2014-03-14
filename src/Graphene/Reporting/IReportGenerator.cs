namespace Graphene.Reporting
{
    public interface IReportGenerator
    {
        /*IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification);*/


        ITrackerReportResults GeneratorReport(IReportSpecification specification);
    }
}