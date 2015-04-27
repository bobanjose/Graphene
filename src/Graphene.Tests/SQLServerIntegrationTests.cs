using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Graphene.Attributes;
using Graphene.Configuration;
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

        private readonly FakeLogger _fakeLogger = new FakeLogger();

        private const string SQLConnectionString =
            @"Server=tcp:hcpif9we3v.database.windows.net;Database=Graphene;User ID=Fr4nk13@hcpif9we3v;Password=(Temp123)_@!;Trusted_Connection=False;Encrypt=True;";
            //@"Server=tcp:[server].database.windows.net;Database=Graphene;User ID=[user];Password=[pass];Trusted_Connection=False;Encrypt=True;";
            //@"Server=.\SQLServer2014;Database=GrapheneV22;Trusted_Connection=True;";  

        private bool _testConfiguratorInitialized;

        private void Initialize()
        {
            if (!_testConfiguratorInitialized)
            {
                Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToSQLServer(SQLConnectionString, _fakeLogger),
                    ReportGenerator = new SQLReportGenerator(SQLConnectionString, _fakeLogger)
                }
                );
                _testConfiguratorInitialized = true;
            }
        }

/*
        private void Shutdown()
        {
            Configurator.ShutDown();
            _testConfiguratorInitialized = false;
        }
*/

        [TestMethod]
        public void IntegrationTest_GivenFilters_AggregatedResultsMatch()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Initialize();

            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(18);
            var report = Container<CustomerVisitTracker>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)),DateTime.UtcNow.Add(new TimeSpan(1, 0, 0))); 
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                report = Container<CustomerVisitTracker>.Where(filter1)
                            .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 * waitCounter, 0)),
                                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));
                
                reportResultCount = report.Results.Count();
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID);
            }
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(1, report.Results[0].Occurrence);
            Assert.AreEqual(18, report.Results[0].Total);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggregatedResultsMatch()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Initialize();

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 17);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 7);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 27);

            var report = Container<TrackerWithCountProperties>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)),DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                report = Container<TrackerWithCountProperties>.Where(filter1)
                        .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 * waitCounter, 0)),
                                DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

                reportResultCount = report.Results.Count();
                Debug.Write("Report Count: " + report.Results.Count() 
                             + " StoreId: " + filter1.StoreID);
            }
            Assert.IsTrue(reportResultCount >= 1);

            var report2 = Container<TrackerWithCountProperties>.Where(new CustomerFilter{Gender = "M"})
                            .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 * waitCounter, 0)), 
                                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report2 Count: " + report2.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");

            Assert.AreEqual(44, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(7, report.Results[0].Tracker.KidsCount);
            
        }

       
        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggregatedResultsMatchForPartialFilters_5MinuteResolution()
        {
            string storeId = Guid.NewGuid().ToString("D");
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };

            Initialize();
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 73);
            Thread.Sleep(new TimeSpan(0, 0, 7, 10));
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 53);
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 23);

            var report = Container<FiveMinuteTrackerWithCountProperties>.Where(new CustomerFilter{StoreID = storeId}).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 2)
            {
                Configurator.FlushTrackers(); 

                report = Container<FiveMinuteTrackerWithCountProperties>
                            .Where(new CustomerFilter{StoreID = storeId})
                            .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), 
                                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

                reportResultCount = report.Results.Count();
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID);
            }
            Assert.IsTrue(report.Results.Count() >= 2);
            Assert.AreEqual(73, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(0, report.Results[0].Tracker.KidsCount);
            Assert.AreEqual(23, report.Results[1].Tracker.ElderlyCount);
            Assert.AreEqual(53, report.Results[1].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggregatedResultsMatchForPartialFilters_HourResolution()
        {
            string storeId = Guid.NewGuid().ToString("D");
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };
            Initialize();
            var startHour = DateTime.UtcNow.Hour;
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 74);
            Thread.Sleep(new TimeSpan(0, 0, 5, 0));
            var endHour = DateTime.UtcNow.Hour;
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 54);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 24);

            var report = Container<TrackerWithCountProperties>.Where(new CustomerFilter{StoreID = storeId}).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 3 && (
                (reportResultCount < 1 && startHour == endHour) || startHour != endHour))
            {
                Configurator.FlushTrackers(); 

                report = Container<TrackerWithCountProperties>
                            .Where(new CustomerFilter{StoreID = storeId})
                            .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 * waitCounter, 0)), 
                                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));


                reportResultCount = report.Results.Count(); 
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID);
            }
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(startHour != endHour ? 24 : 98, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(54, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_WithoutFiltersAndUsingNamedTrackers_AggregatedResultsMatch()
        {
            Initialize();
            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 15);
            Container<TrackerWithCountProperties>.Increment(t => t.KidsCount, 55);
            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 5);

            var report = Container<TrackerWithCountProperties>.Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 6, 0)));
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                report = Container<TrackerWithCountProperties>
                                .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6*waitCounter, 0)),
                                        DateTime.UtcNow.Add(new TimeSpan(1, 6, 0)));
                
                reportResultCount = report.Results.Count();
                Debug.Write("Report Count: " + report.Results.Count()
                            + " StoreId: <EmptyString>  ");
            }

            Assert.IsTrue(report.Results.Any());
            Assert.IsTrue(report.Results[0].Tracker.ElderlyCount >= 20);
            Assert.IsTrue(report.Results[0].Tracker.KidsCount >= 55);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackers_AggregatedResultsMatchForPartialFiltersWithMultipleRecordsDefault
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };
            
            Initialize();
            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(16);

            var report = Container<CustomerVisitTracker>.Where(new CustomerFilter { Gender = "M", }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Year);
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                report = Container<CustomerVisitTracker>.Where(new CustomerFilter{Gender = "M",})
                            .Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                    DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), 
                                    ReportResolution.Year);

                reportResultCount = report.Results.Count(); 
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID + "  ");
            }

            Assert.IsTrue(report.Results[0].Total >= 16);
            Assert.IsTrue(report.Results[0].Occurrence >= 1);
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggregatedResultsMatchForPartialFiltersWithMultipleRecordsMulitpleResolution
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Initialize();
            var now = DateTime.UtcNow;
            Container<PerformanceTracker>.Where(filter1).IncrementBy(11);

            var report = Container<PerformanceTracker>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Minute);
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                //minute
                report = Container<PerformanceTracker>.Where(filter1)
                        .Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), 
                                ReportResolution.Minute);
                
                reportResultCount = report.Results.Count(); 
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID + "  ");
            }

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(11, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Minute, report.Resolution);

            //five minute
            report = Container<PerformanceTracker>.Where(filter1)
                        .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                now.Add(new TimeSpan(1, 0, 0)), 
                                ReportResolution.FiveMinute);

            Debug.Write("Report2 Count: " + report.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(11, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.FiveMinute, report.Resolution);
            
            //day
            report = Container<PerformanceTracker>.Where(filter1)
                        .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                now.Add(new TimeSpan(1, 0, 0)),
                                ReportResolution.Day);

            Debug.Write("Report3 Count: " + report.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(now.Day, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(11, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Day, report.Resolution);

            //month
            report = Container<PerformanceTracker>.Where(filter1)
                        .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                now.Add(new TimeSpan(1, 0, 0)),
                                ReportResolution.Month);

            Debug.Write("Report4 Count: " + report.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");
            
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(now.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(11, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Month, report.Resolution);

            //year
            report = Container<PerformanceTracker>.Where(filter1)
                        .Report(now.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                now.Add(new TimeSpan(1, 0, 0)), 
                                ReportResolution.Year);

            Debug.Write("Report5 Count: " + report.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");
            
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(now.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(11, report.Results[0].Total);
            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void
            IntegrationTest_GivenFiltersAndNamedTrackersWith5MinuteResolution_AggregatedResultsMatchForPartialFiltersWithMultipleRecordsDefaultResolution
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Initialize();
            Container<PerformanceTracker>.Where(filter1).IncrementBy(12);

            var report = Container<PerformanceTracker>.Where(new CustomerFilter{Gender = "M",}).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),DateTime.Now.Add(new TimeSpan(1, 0, 0)), ReportResolution.Year);
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers(); 

                report = Container<PerformanceTracker>.Where(new CustomerFilter {Gender = "M",})
                         .Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),
                                 DateTime.Now.Add(new TimeSpan(1, 0, 0)),
                                 ReportResolution.Year);

                reportResultCount = report.Results.Count();
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID + "  ");
            }
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(DateTime.UtcNow.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Minute);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Hour);

            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void IntegrationTest_GivenFiltersAndNamedTrackers_AggregatedResultsMatch_WithTimeOffset()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Initialize();
            var startHour = DateTime.UtcNow.Hour;
            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(19);

            var report = Container<CustomerVisitTracker>.Where(filter1)
                .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Hour);
            var waitCounter = 0;
            var reportResultCount = 0;
            while (waitCounter++ < 2 && reportResultCount < 1)
            {
                Configurator.FlushTrackers();
                report = Container<CustomerVisitTracker>.Where(filter1)
                    .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 * waitCounter, 0)),
                            DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)),
                            ReportResolution.Hour);

                reportResultCount = report.Results.Count(); 
                Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID + "  ");
            }
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(ReportResolution.Hour, report.Resolution);

            var endHour = DateTime.UtcNow.Hour;
            report = Container<CustomerVisitTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(((startHour != endHour)? 2:1), 6 * waitCounter, 0)),
                      DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Hour, 
                      new TimeSpan(-7, 0, 0));

            Debug.Write("Report2 Count: " + report.Results.Count() 
                        + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any()); 
            Assert.IsTrue(report.Results[0].Total >= 19);
            Assert.IsTrue(report.Results[0].Occurrence >= 1);
            Assert.AreEqual(ReportResolution.Hour, report.Resolution);
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

        public class FiveMinuteTrackerWithCountProperties : ITrackable
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
                get { return Resolution.FiveMinute; }
            }
        }

        public class MinuteTrackerWithCountProperties : ITrackable
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
                get { return Resolution.Minute; }
            }
        }
    }
}
