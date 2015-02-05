using System;
using System.Linq;
using System.Threading;
using Graphene.Attributes;
using Graphene.Configuration;
using Graphene.Mongo.Reporting;
using Graphene.Publishing;
using Graphene.Reporting;
using Graphene.SQLServer;
using Graphene.Tests.Fakes;
using Graphene.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Graphene.Tests.Reporting
{
    [TestClass]
    public class SQLReportingTests
    {
        //*****************************************************************************************************************
        //Most of the tests below are integration type tests and can only be run in isolation (individually)
        //*****************************************************************************************************************

        private FakeLogger _fakeLogger = new FakeLogger();

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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);


            //Container<TrackerWithCountProperties>.Where(new CustomerFilter
            //{
            //    Environment_ServerName = "Env1",
            //    Gender = "M",
            //    State = "MN",
            //    StoreID = Guid.NewGuid().ToString("D")
            //}).Increment(t => t.ElderlyCount, 2)
            //.Increment(t => t.MiddleAgedCount, 1);

            Configurator.ShutDown();

            var report =
                Container<TrackerWithCountProperties>.Where(filter1)
                    .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results.Count() >= 1);


            var report2 =
                Container<TrackerWithCountProperties>.Where(
                new CustomerFilter
                {
                    Gender = "M"
                }).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
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

            System.Diagnostics.Debug.Write(report.Results.Count());

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(2, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(5, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_WithoutFiltersAndUsingNamedTrackers_AggreagetedResultsMatch()
        {

            Configurator.Initialize(
                new Settings
                {
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
                }
                );

            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            AggregationResults<CustomerVisitTracker> report = Container<CustomerVisitTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Assert.IsTrue(report.Results[0].Total >= 10);
            Assert.IsTrue(report.Results[0].Occurrence >= 1);
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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
                }
                );

            var now = DateTime.UtcNow;
            Container<PerformanceTracker>.Where(filter1).IncrementBy(10);

            Configurator.ShutDown();

            //minute
            AggregationResults<PerformanceTracker> report = Container<PerformanceTracker>.Where(filter1)
                .Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Minute);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Minute, report.Resolution);

            //five minute
            report = Container<PerformanceTracker>.Where(filter1)
                .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    now.Add(new TimeSpan(1, 0, 0)), ReportResolution.FiveMinute);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.FiveMinute, report.Resolution);

            //30 minute
            report = Container<PerformanceTracker>.Where(filter1)
                .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    now.Add(new TimeSpan(1, 0, 0)), ReportResolution.ThirtyMinute);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.ThirtyMinute, report.Resolution);

            //day
            report = Container<PerformanceTracker>.Where(filter1)
                .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    now.Add(new TimeSpan(1, 0, 0)), ReportResolution.Day);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Day, report.Resolution);

            //month
            report = Container<PerformanceTracker>.Where(filter1)
                .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    now.Add(new TimeSpan(1, 0, 0)), ReportResolution.Month);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Month, report.Resolution);

            report = Container<PerformanceTracker>.Where(filter1)
                .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                    now.Add(new TimeSpan(1, 0, 0)), ReportResolution.Year);

            Assert.IsTrue(report.Results.Count() >= 1);
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(10, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Year, report.Resolution);
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
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;", _fakeLogger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV20;Trusted_Connection=True;",
                            _fakeLogger)
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

        public class TrackerWithCountProperties : ITrackable
        {
            [Measurement]
            public long KidsCount { get; set; }

            [Measurement]
            public long MiddleAgedCount { get; set; }

            [Measurement]
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
