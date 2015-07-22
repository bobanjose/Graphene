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

namespace Graphene.Tests
{
    [TestClass]
    public class sql_reporting_tests
    {
        //*****************************************************************************************************************
        //Most of the tests below are integration type tests and can only be run in isolation (individually)
        //*****************************************************************************************************************

        private static readonly FakeLogger FakeLogger = new FakeLogger();

        private const string SQL_CONNECTION_STRING =
            //@"Server=tcp:[server].database.windows.net;Database=Graphene;User ID=[user];Password=[pass];Trusted_Connection=False;Encrypt=True;";
            @"Server=.\MSSQLSERVER2014;Database=Graphene;Trusted_Connection=True;"; 

        private static bool _testConfiguratorInitialized;
        

        [ClassInitialize]
        public static void Init(TestContext testContext)
        {
            if (!_testConfiguratorInitialized)
            {
                Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToSQLServer(SQL_CONNECTION_STRING, FakeLogger),
                    ReportGenerator = new SQLReportGenerator(SQL_CONNECTION_STRING, FakeLogger)
                });
                _testConfiguratorInitialized = true;
            }
        }

        [TestMethod]
        public void integration_test_given_filters_aggregated_results_match()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(18);
            Configurator.FlushTrackers(); 

            var report = Container<CustomerVisitTracker>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)),DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report Count: " + report.Results.Count() 
                            + " StoreId: " + filter1.StoreID);
            
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(1, report.Results[0].Occurrence);
            Assert.AreEqual(18, report.Results[0].Total);
        }

        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_aggregated_results_match()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 17);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 7);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 27);

            Configurator.FlushTrackers();

            var report = Container<TrackerWithCountProperties>.Where(new CustomerFilter { Gender = "M", StoreID = filter1.StoreID})
                           .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6 , 0)),
                                   DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report Count: " + report.Results.Count()
                        + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(44, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(7, report.Results[0].Tracker.KidsCount);
        }

       
        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_aggregated_results_match_for_partial_filters_5_minute_resolution()
        {
            string storeId = Guid.NewGuid().ToString("D");
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };
           
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 73);
            Thread.Sleep(new TimeSpan(0, 0, 7, 10));
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 53);
            Container<FiveMinuteTrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 23);

            Configurator.FlushTrackers(); 

            var report = Container<FiveMinuteTrackerWithCountProperties>.Where(new CustomerFilter{StoreID = storeId}).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report Count: " + report.Results.Count()
                         + " StoreId: " + filter1.StoreID);

            Assert.IsTrue(report.Results.Count() >= 2);
            Assert.AreEqual(73, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(0, report.Results[0].Tracker.KidsCount);
            Assert.AreEqual(23, report.Results[1].Tracker.ElderlyCount);
            Assert.AreEqual(53, report.Results[1].Tracker.KidsCount);
           
        }

        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_aggregated_results_match_for_partial_filters_hour_resolution()
        {
            string storeId = Guid.NewGuid().ToString("D");
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = storeId
            };
            var startHour = DateTime.UtcNow.Hour;
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 74);
            Thread.Sleep(new TimeSpan(0, 0, 5, 0));
            var endHour = DateTime.UtcNow.Hour;
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 54);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 24);

            Configurator.FlushTrackers(); 

            var report = Container<TrackerWithCountProperties>.Where(new CustomerFilter{StoreID = storeId}).Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report Count: " + report.Results.Count()
                           + " StoreId: " + filter1.StoreID);

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(startHour != endHour ? 24 : 98, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(54, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void integration_test_without_filters_and_using_named_trackers_aggregated_results_match()
        {
            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 15);
            Container<TrackerWithCountProperties>.Increment(t => t.KidsCount, 55);
            Container<TrackerWithCountProperties>.Increment(t => t.ElderlyCount, 5);

            Configurator.FlushTrackers(); 

            var report = Container<TrackerWithCountProperties>.Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)));

            Debug.Write("Report Count: " + report.Results.Count()
                            + " StoreId: <EmptyString>  ");

            Assert.IsTrue(report.Results.Any());
            Assert.IsTrue(report.Results[0].Tracker.ElderlyCount >= 20);
            Assert.IsTrue(report.Results[0].Tracker.KidsCount >= 55);
        }

        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_aggregated_results_match_for_partial_filters_with_multiple_records_default
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };
            
            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(16);

            Configurator.FlushTrackers(); 

            var report = Container<CustomerVisitTracker>.Where(new CustomerFilter { Gender = "M", }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Year);

            Debug.Write("Report Count: " + report.Results.Count()
                           + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results[0].Total >= 16);
            Assert.IsTrue(report.Results[0].Occurrence >= 1);
            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_with5_minute_resolution_aggregated_results_match_for_partial_filters_with_multiple_records_mulitple_resolution()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };

            var now = DateTime.UtcNow;
            
            Container<PerformanceTracker>.Where(filter1).IncrementBy(11);
            Configurator.FlushTrackers(); 

            var report = Container<PerformanceTracker>.Where(filter1).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)),DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Minute, TimeSpan.Zero);

            Debug.Write("Report Count: " + report.Results.Count()
                          + " StoreId: " + filter1.StoreID + "  ");

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
        public void integration_test_given_filters_and_named_trackers_with5_minute_resolution_aggregated_results_match_for_partial_filters_with_multiple_records_default_resolution
            ()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };
         
            Container<PerformanceTracker>.Where(filter1).IncrementBy(12);

            Configurator.FlushTrackers();

            var report = Container<PerformanceTracker>.Where(new CustomerFilter { Gender = "M", }).Report(DateTime.UtcNow.Subtract(new TimeSpan(5000, 1, 0, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Year);
            Debug.Write("Report Count: " + report.Results.Count()
                            + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(DateTime.UtcNow.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(1, report.Results[0].MesurementTimeUtc.Day);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Minute);
            Assert.AreEqual(0, report.Results[0].MesurementTimeUtc.Hour);

            Assert.AreEqual(ReportResolution.Year, report.Resolution);
        }

        [TestMethod]
        public void integration_test_given_filters_and_named_trackers_aggregated_results_match_with_time_offset()
        {
            var filter1 = new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "CA",
                StoreID = Guid.NewGuid().ToString("D")
            };
           
            var startHour = DateTime.UtcNow.Hour;
            Container<CustomerVisitTracker>.Where(filter1).IncrementBy(19);

            Configurator.FlushTrackers();

            var report = Container<CustomerVisitTracker>.Where(filter1)
                .Report(DateTime.UtcNow.Subtract(new TimeSpan(1, 6, 0)), DateTime.UtcNow.Add(new TimeSpan(1, 0, 0)), ReportResolution.Hour);

            Debug.Write("Report Count: " + report.Results.Count()
                           + " StoreId: " + filter1.StoreID + "  ");

            Assert.IsTrue(report.Results.Any());
            Assert.AreEqual(ReportResolution.Hour, report.Resolution);

         
            var endHour = DateTime.UtcNow.Hour;
            report = Container<CustomerVisitTracker>.Where(new CustomerFilter
            {
                Gender = "M",
            }).Report(DateTime.UtcNow.Subtract(new TimeSpan(((startHour != endHour)? 2:1), 6, 0)),
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
