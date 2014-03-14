


using Graphene.Attributes;

namespace Graphene.Mongo.IntegrationTests
{
    using System;
    using CommonWell.Framework.Reporting.Trackers;
    using Graphene.Mongo.Reporting;
    using Graphene.Reporting;
    using Graphene.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            MongoReportGenerator generator = new MongoReportGenerator("mongodb://localhost:9001/Graphene");

            GrapheneReportSpecification spec = new GrapheneReportSpecification(new Type[] { typeof(PatientDemographicSearchMatchesTracker)} , DateTime.UtcNow.AddDays(-100), DateTime.UtcNow, ReportResolution.Hour);

            

           var newresult = generator.GeneratorReport(spec);
        }


        public class CustomerAgeTracker : ITrackable
        {
            public string Name { get { return "Customer Age Tracker"; } }

            public string Description { get { return "Counts the number of customer visits"; } }

            public Resolution MinResolution { get { return Resolution.Hour; } }

            public long KidsCount { get; set; }
            public long MiddleAgedCount { get; set; }
            public long ElderlyCount { get; set; }
        }
    }
}

namespace CommonWell.Framework.Reporting.Trackers
{
    using Graphene.Tracking;
    public class PatientDemographicSearchMatchesTracker : ITrackable
    {
        public string Name { get { return "Patient Demographic Search Tracker"; } }
        public string Description { get { return "Holds metrics related to the Patient search functionality using Demographics"; } }
        public Resolution MinResolution { get { return Resolution.Hour; } }

        [Measurable("Matches with Scores Greater than 90%", "Counts all matches with a score greater than 90%.")]
        public long MatchesWithScoresGreaterThan90 { get; set; }
        public long MatchesWithScoresBetween80And90 { get; set; }
        public long MatchesWithScoresBetween75And80 { get; set; }
        public long MatchesWithScoresBetween70And75 { get; set; }
        public long MatchesWithScoresBetween65And70 { get; set; }
        public long MatchesWithScoresBetween60And65 { get; set; }
        public long MatchesWithScoresBelow60 { get; set; }

        public long SearchRequests { get; set; }
    }
}
