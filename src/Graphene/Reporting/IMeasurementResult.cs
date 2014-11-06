namespace Graphene.Reporting
{
    public interface IMeasurementResult : IMeasurement
    {
        long Value { get; }
    }
}