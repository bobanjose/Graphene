namespace Graphene.Reporting
{
    public interface IMeasurement
    {
        string PropertyName { get; }

        string TrackerTypeName { get; }

        string DisplayName { get; }

        string Description { get; }

        string FullyQualifiedPropertyName { get; }
    }

    public interface IMeasurementResult : IMeasurement
    {
        string Value { get; }
    }
}