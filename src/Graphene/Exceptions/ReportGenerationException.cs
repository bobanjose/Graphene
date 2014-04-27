using System;

namespace Graphene.Exceptions
{
    public class ReportGenerationException : Exception
    {
        private const string message =
            "There was a problem processing the request for a Report.  See inner exception, if any, for details.";

        public ReportGenerationException(Exception innerException) : base(message, innerException)
        {
        }
    }
}