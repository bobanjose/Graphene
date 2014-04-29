using System;
using System.Linq;
using CommonWell.Framework.Reporting.Trackers;
using Graphene.Attributes;
using Graphene.Configuration;
using Graphene.Mongo.Reporting;
using Graphene.Reporting;
using Graphene.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Graphene.Mongo.IntegrationTests
{
    [TestClass]
    public class MongoReportGeneratorTests
    {

        public class FakeLogger : ILogger
        {
            public void Debug(string message)
            {
            
            }

            public void Info(string message)
            {
            
            }

            public void Warn(string message)
            {
            
            }

            public void Error(string message, Exception ex)
            {
            
            }
        }

        private FakeLogger  _fakeLogger = new FakeLogger();

        //[TestMethod] This is a bad test. This assumes there is already data in Graphene
        public void GivenATrackerWithNoCustomFields_WhenIncrementingTraker_ThenTotalReflectsIncrementAndNoOccurencesIsSet()
        {


            var generator = new MongoReportGenerator("mongodb://localhost:9001/Graphene", _fakeLogger);

            var spec = new ReportSpecification(new[] {typeof (PatientDemographicSearchMatchesTracker)},
                DateTime.UtcNow.AddDays(-100), DateTime.UtcNow, ReportResolution.Hour);


            ITrackerReportResults newresult = generator.BuildReport(spec);

            Assert.IsTrue(newresult.AggregationResults.Any());


        }


        public class CustomerAgeTracker : ITrackable
        {
            public long KidsCount { get; set; }
            public long MiddleAgedCount { get; set; }
            public long ElderlyCount { get; set; }

            public string Name
            {
                get { return "Customer Age Tracker"; }
            }

            public string Description
            {
                get { return "Counts the number of customer visits"; }
            }

            public Resolution MinResolution
            {
                get { return Resolution.Hour; }
            }
        }
    }
}

namespace CommonWell.Framework.Reporting.Trackers
{
    public class PatientDemographicSearchMatchesTracker : ITrackable
    {
        [Measurable("Matches with Scores Greater than 90%", "Counts all matches with a score greater than 90%.")]
        public long MatchesWithScoresGreaterThan90 { get; set; }

        public long MatchesWithScoresBetween80And90 { get; set; }
        public long MatchesWithScoresBetween75And80 { get; set; }
        public long MatchesWithScoresBetween70And75 { get; set; }
        public long MatchesWithScoresBetween65And70 { get; set; }
        public long MatchesWithScoresBetween60And65 { get; set; }
        public long MatchesWithScoresBelow60 { get; set; }

        public long SearchRequests { get; set; }

        public string Name
        {
            get { return "Patient Demographic Search Tracker"; }
        }

        public string Description
        {
            get { return "Holds metrics related to the Patient search functionality using Demographics"; }
        }

        public Resolution MinResolution
        {
            get { return Resolution.Hour; }
        }
    }
}