using System;
using System.Linq;
using System.Threading;
using Graphene.Attributes;
using Graphene.Configuration;
using Graphene.Mongo.Reporting;
using Graphene.Publishing;
using Graphene.Reporting;
using Graphene.Tests.Fakes;
using Graphene.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Graphene.Tests.Reporting
{
    [TestClass]
    public class QueryTest
    {
        //Most of the tests below are integration type tests and can only be run in isolation (individually)

        private FakeLogger _fakeLogger = new FakeLogger();

        [TestMethod]
        public void GivenAQueryWithTwoFilters_WhenBuildingTheList_AppropriateFiltersAreConverted()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = "1234"
            };

            var filter2 = new CustomerFilter
            {
                Environment_ServerName = "Env2",
                Gender = "F",
                State = "CT",
                StoreID = "4231"
            };

            var visitTrackerReportSpecification =
                new ReportSpecification<CustomerFilter, CustomerVisitTracker>(DateTime.Now, DateTime.UtcNow,
                    ReportResolution.Minute, filter1, filter2);

            Assert.AreEqual(2, visitTrackerReportSpecification.FilterCombinations.Count());
            Assert.AreEqual(4, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
        }

        public void
            GivenAQueryWithTrackerProperties_WhenBuildingTheListOfTrackersToTrack_OnlyTheAppropriateCountersAreCounted()
        {
            var visitTrackerReportSpecification =
                new ReportSpecification<CustomerFilter, CustomerAgeTracker>(DateTime.Now, DateTime.UtcNow,
                    ReportResolution.Minute);

            Assert.AreEqual(3, visitTrackerReportSpecification.Counters.Count());
        }

        [TestMethod]
        public void IntegrationTest_GivenFilters_AggreagetedResultsMatch()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene",_fakeLogger)
                }
                );

            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            AggregationResults<CustomerVisitTracker> report =
                Container<CustomerVisitTracker>.Where(filter1)
                    .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(1, report.Results[0].Occurrence);
            Assert.AreEqual(10, report.Results[0].Total);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatch()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene", _fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);
            Configurator.ShutDown();

            AggregationResults<TrackerWithCountProperties> report =
                Container<TrackerWithCountProperties>.Where(filter1)
                    .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(12, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(5, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatchForPartialFilters()
        {
            string storeId = Guid.NewGuid().ToString("D");
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene", _fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 0);
            Thread.Sleep(new TimeSpan(0, 0, 5, 0));
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);
            
            Configurator.ShutDown();

            AggregationResults<TrackerWithCountProperties> report = Container<TrackerWithCountProperties>.Where(new CustomerFilter
            {
                StoreID = storeId
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(12, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(5, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_WithoutFiltersAndUsingNamedTrackers_AggreagetedResultsMatch()
        {
            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene",_fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 10);
            Container<TrackerWithCountProperties>.Increment(t => t.KidsCount, 5);
            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 2);
            Configurator.ShutDown();

            AggregationResults<TrackerWithCountProperties> report =
                Container<TrackerWithCountProperties>.Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)),
                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.IsTrue(report.Results[0].Tracker.ElderlyCount >= 12);
            Assert.IsTrue(report.Results[0].Tracker.KidsCount >= 5);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsDefault
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene", _fakeLogger)
                }
                );

            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            AggregationResults<CustomerVisitTracker> report = Container<CustomerVisitTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsMinuteResolution
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene", _fakeLogger)
                }
                );

            Container<PerformanceTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            AggregationResults<PerformanceTracker> report = Container<PerformanceTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            })
                .Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Minute);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(DateTime.Now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(DateTime.Now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(DateTime.Now.Day, report.Results[0].MesurementTimeUtc.Day);

            Assert.AreEqual(ReportResolution.Minute, report.Resolution);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsDefaultResolution
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene"),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:9001/Graphene", _fakeLogger)
                }
                );

            Container<PerformanceTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            AggregationResults<PerformanceTracker> report = Container<PerformanceTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(DateTime.Now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Minute);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Hour);

            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestClass]
        public class ReportSpecificationTests
        {
            [TestMethod]
            public void GivenAQueryWithTwoFilters_WhenBuildingTheList_AppropriateFiltersAreConverted()
            {
                var filter1 = new CustomerFilter
                {
                    Environment_ServerName = "Env1",
                    Gender = "M",
                    State = "CA",
                    StoreID = "1234"
                };

                var filter2 = new CustomerFilter
                {
                    Environment_ServerName = "Env2",
                    Gender = "F",
                    State = "CT",
                    StoreID = "4231"
                };

                var visitTrackerReportSpecification =
                    new ReportSpecification<CustomerFilter, CustomerVisitTracker>(DateTime.Now, DateTime.UtcNow,
                        ReportResolution.Day, filter1, filter2);

                Assert.AreEqual(2, visitTrackerReportSpecification.FilterCombinations.Count());
                Assert.AreEqual(4, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
                Assert.AreEqual(
                    string.Format("{0}::{1}",
                        ("Environment_ServerName").ToUpper(), filter1.Environment_ServerName.ToUpper()),
                    visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.ElementAt(0));
            }

            [TestMethod]
            public void
                GivenAQueryWithTrackerProperties_WhenBuildingTheListOfTrackersToTrack_OnlyTheAppropriateCountersAreCounted
                ()
            {
                var visitTrackerReportSpecification =
                    new ReportSpecification<CustomerFilter, TrackerWithCountProperties>(DateTime.Now, DateTime.UtcNow,
                        ReportResolution.Day);

                //Assert.AreEqual(4, visitTrackerReportSpecification.Counters.Count());
            }
        }

        public class TrackerWithCountProperties : ITrackable
        {
            [Measurable]
            public long KidsCount { get; set; }

            [Measurable]
            public long MiddleAgedCount { get; set; }

            [Measurable]
            public long ElderlyCount { get; set; }

            public long NotACounter { get; set; }

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