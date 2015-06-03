namespace Graphene.Reporting
{
    public interface IMeasurementResult : IMeasurement
    {
        string Value { get; }
    }
}