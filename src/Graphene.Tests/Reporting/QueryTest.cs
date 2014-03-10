using System;
using System.Linq;
using Graphene.Reporting;
using Graphene.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Graphene.Tests.Reporting
{
    [TestClass]
    public class QueryTest
    {
        //Most of the tests below are integration type tests and can only be run in isolation (individually)

        [TestMethod]
        public void GivenAQueryWithTwoFilters_WhenBuildingTheList_AppropriateFiltersAreConverted()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = "1234"
            };

            CustomerFilter filter2 = new CustomerFilter
            {
                Environment_ServerName = "Env2",
                Gender = "F",
                State = "CT",
                StoreID = "4231"
            };

            var visitTrackerReportSpecification = new ReportSpecification<CustomerFilter, CustomerVisitTracker>(DateTime.Now, DateTime.UtcNow, ReportResolution.Minute, filter1, filter2);

            Assert.AreEqual(2, visitTrackerReportSpecification.FilterCombinations.Count());
            Assert.AreEqual(1, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
        }

        public void GivenAQueryWithTrackerProperties_WhenBuildingTheListOfTrackersToTrack_OnlyTheAppropriateCountersAreCounted()
        {

            var visitTrackerReportSpecification = new ReportSpecification<CustomerFilter, CustomerAgeTracker>(DateTime.Now, DateTime.UtcNow, ReportResolution.Minute);

            Assert.AreEqual(3, visitTrackerReportSpecification.Counters.Count());

        }

        [TestMethod]
        public void IntegrationTest_GivenFilters_AggreagetedResultsMatch()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<CustomerVisitTracker>.Where(filter1).IncrementBy(10);

            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<CustomerVisitTracker>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1,0,0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(1, report.Results[0].Occurrence);
            Assert.AreEqual(10, report.Results[0].Total);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatch()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);
            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(12, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(5, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatchForPartialFilters()
        {
            var storeId = Guid.NewGuid().ToString("D");
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);
            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Where(new CustomerFilter
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
            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 10);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Increment(t => t.KidsCount, 5);
            Graphene.Tracking.Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 2);
            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.IsTrue(report.Results[0].Tracker.ElderlyCount >= 12);
            Assert.IsTrue(report.Results[0].Tracker.KidsCount >= 5);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsDefault()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<CustomerVisitTracker>.Where(filter1).IncrementBy(10);

            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<CustomerVisitTracker>.Where(new CustomerFilter
            {
                Gender = "M",             
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(ReportResolution.Year, report.Resolution); 
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsMinuteResolution()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<PerformanceTracker>.Where(filter1).IncrementBy(10);

            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<PerformanceTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Minute);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(DateTime.Now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(DateTime.Now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(DateTime.Now.Day, report.Results[0].MesurementTimeUtc.Day);

            Assert.AreEqual(ReportResolution.Minute, report.Resolution);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggreagetedResultsMatchForPartialFiltersWithMultipleRecordsDefaultResolution()
        {
            CustomerFilter filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );

            Graphene.Tracking.Container<PerformanceTracker>.Where(filter1).IncrementBy(10);

            Graphene.Configurator.ShutDown();

            var report = Graphene.Tracking.Container<PerformanceTracker>.Where(new CustomerFilter
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
    }

     public class TrackerWithCountProperties : ITrackable
     {
         public string Name { get { return "Customer Age Tracker"; } }

         public string Description { get { return "Counts the number of customer visits"; } }

         public Resolution MinResolution { get { return Resolution.Hour; } }

         public long KidsCount { get; set; }
         public long MiddleAgedCount { get; set; }
         public long ElderlyCount { get; set; }
     }

}