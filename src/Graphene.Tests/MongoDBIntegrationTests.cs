// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Graphene.Attributes;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Mongo.Publishing;
using Graphene.Mongo.Reporting;
using Graphene.Publishing;
using Graphene.Reporting;
using Graphene.SQLServer;
using Graphene.Tests.Fakes;
using Graphene.Tests.Reporting;
using Graphene.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Graphene.Tests
{
    public class FakePersister : IPersist
    {
        private object _lock = new object();
        private List<TrackerData> _trackingDate = new List<TrackerData>();

        public async void Persist(TrackerData trackerData)
        {
        }

        public bool PersistPreAggregatedBuckets { get; set; }
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

    public class CustomerVisitTracker : ITrackable
    {
        public string Name
        {
            get { return "Customer Visit Tracker"; }
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

    public class CustomerPurchaseTracker : ITrackable
    {
        public string Name
        {
            get { return "Customer Purchase Tracker"; }
        }

        public string Description
        {
            get { return "Counts the number of customer purchases"; }
        }

        public Resolution MinResolution
        {
            get { return Resolution.Hour; }
        }
    }

    public class PerformanceTracker : ITrackable
    {
        public int NumberOfCalls { get; set; }

        public long TotalResponseTimeInMilliseconds { get; set; }

        public string Name
        {
            get { return "Method A Performance Tracker"; }
        }

        public string Description
        {
            get { return "Tracks the response time of ### method"; }
        }

        public Resolution MinResolution
        {
            get { return Resolution.FiveMinute; }
        }
    }

    public struct CustomerFilter
    {
        public string State { get; set; }
        public string StoreID { get; set; }
        public string Gender { get; set; }
        public string Environment_ServerName { get; set; }
    }

    public struct EnvironmentFilter
    {
        public string ServerName { get; set; }
    }

    [TestClass]
    public class MongoDBIntegrationTests
    {
        //*****************************************************************************************************************
        //Most of the tests below are integration type tests and can only be run in isolation (individually)
        //*****************************************************************************************************************

        private FakeLogger _fakeLogger = new FakeLogger();


        private static int _task1Count;
        private static int _task2Count;
        private static int _task3Count;


        [ClassInitialize]
        public static void Init(TestContext testContext)
        {
            Configurator.Initialize(
                new Settings {Persister = new PersistToMongo("mongodb://localhost/Graphene", new FakeLogger())}
                );
        }

        [ClassCleanup]
        public static void ShutDown()
        {
            Configurator.ShutDown();
        }

        [TestMethod]
        public void TestEmpty()
        {
        }

        [TestMethod]
        public void TestIncrement()
        {
            var ct = new CancellationTokenSource();

            Task task1 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    //Graphene.Tracking.Container<PatientLinkValidationTracker>.IncrementBy(1);
                    Container<CustomerVisitTracker>
                        .Where(
                            new CustomerFilter
                            {
                                StoreID = "3234",
                                Environment_ServerName = "Server1"
                            }).IncrementBy(1);
                    _task1Count++;
                    // System.Threading.Thread.Sleep(500);
                }
            }, ct.Token);

            Task task2 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Container<CustomerPurchaseTracker>
                        .Where(
                            new CustomerFilter
                            {
                                State = "MN",
                                StoreID = "334",
                                Environment_ServerName = "Server2"
                            }).IncrementBy(1);
                    _task2Count++;
                    // System.Threading.Thread.Sleep(100);
                }
            }, ct.Token);

            Task task3 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Container<CustomerVisitTracker>.IncrementBy(3);
                    _task3Count++;
                    //System.Threading.Thread.Sleep(500);
                }
            }, ct.Token);

            Thread.Sleep(1000);

            ct.Cancel();

            Task.WaitAll(task1, task2, task3);
        }

        [TestMethod]
        public void TestNamedMetrics()
        {
            var ct = new CancellationTokenSource();

            Task task1 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Container<CustomerAgeTracker>
                        .Where(
                            new CustomerFilter
                            {
                                State = "MN",
                                StoreID = "334",
                                Environment_ServerName = "Server2"
                            })
                        .Increment(e => e.MiddleAgedCount, 1)
                        .Increment(e => e.ElderlyCount, 2);
                }
            }, ct.Token);

            Task task2 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Container<CustomerAgeTracker>
                        .Increment(e => e.KidsCount, 2);
                }
            }, ct.Token);


            Thread.Sleep(1000);

            ct.Cancel();

            Task.WaitAll(task1, task2);
        }

        [TestMethod]
        public void TestPerformaceMetrics()
        {
            var ct = new CancellationTokenSource();

            Task task1 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Stopwatch sw = Stopwatch.StartNew();

                    Thread.Sleep(Convert.ToInt32((new Random()).NextDouble()*100) + 5);

                    sw.Stop();

                    Container<PerformanceTracker>
                        .Where(
                            new EnvironmentFilter
                            {
                                ServerName = "Server2"
                            })
                        .Increment(e => e.NumberOfCalls, 1)
                        .Increment(e => e.TotalResponseTimeInMilliseconds, sw);

                    sw.Reset();
                    sw.Start();
                    Thread.Sleep(Convert.ToInt32((new Random()).NextDouble()*100) + 5);
                    sw.Stop();

                    Container<PerformanceTracker>
                        .Where(
                            new EnvironmentFilter
                            {
                                ServerName = "Server1"
                            })
                        .Increment(e => e.NumberOfCalls, 1)
                        .Increment(e => e.TotalResponseTimeInMilliseconds, sw);
                }
            }, ct.Token);

            Task task2 = Task.Run(() =>
            {
                while (!ct.IsCancellationRequested)
                {
                    Stopwatch sw = Stopwatch.StartNew();

                    Thread.Sleep(Convert.ToInt32((new Random()).NextDouble()*100) + 5);

                    sw.Stop();

                    Container<PerformanceTracker>
                        .Increment(e => e.NumberOfCalls, 1)
                        .Increment(e => e.TotalResponseTimeInMilliseconds, sw);
                }
            }, ct.Token);


            Thread.Sleep(1000);

            ct.Cancel();

            Task.WaitAll(task1, task2);
        }

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
            Assert.AreEqual(1, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
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
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene", _fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene", _fakeLogger)
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
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene1",_fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene1", _fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.KidsCount, 5);
            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 2);


            Container<TrackerWithCountProperties>.Where(new CustomerFilter
            {
                Environment_ServerName = "Env1",
                Gender = "M",
                State = "MN",
                StoreID = Guid.NewGuid().ToString("D")
            }).Increment(t => t.ElderlyCount, 2)
            .Increment(t => t.MiddleAgedCount,1);

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
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene1",_fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene1", _fakeLogger)
                }
                );

            Container<TrackerWithCountProperties>.Where(filter1).Increment(t => t.ElderlyCount, 10);
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
            Assert.AreEqual(12, report.Results[0].Tracker.ElderlyCount);
            Assert.AreEqual(5, report.Results[0].Tracker.KidsCount);
        }

        [TestMethod]
        public void IntegrationTest_WithoutFiltersAndUsingNamedTrackers_AggreagetedResultsMatch()
        {
            Configurator.Initialize(
                new Settings
                {
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene",_fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene",_fakeLogger)
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
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene",_fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene", _fakeLogger)
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
                    Persister = new PersistToMongo("mongodb://localhost:27017/Graphene",_fakeLogger),
                    ReportGenerator = new MongoReportGenerator("mongodb://localhost:27017/Graphene", _fakeLogger)
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
            Assert.AreEqual(DateTime.UtcNow.Year, report.Results[0].MesurementTimeUtc.Year);
            Assert.AreEqual(DateTime.UtcNow.Month, report.Results[0].MesurementTimeUtc.Month);
            Assert.AreEqual(DateTime.UtcNow.Day, report.Results[0].MesurementTimeUtc.Day);

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
                    Persister = new PersistToMongo("mongodb://localhost:9001/Graphene",_fakeLogger),
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
            Assert.AreEqual(DateTime.UtcNow.Year, report.Results[0].MesurementTimeUtc.Year);
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
                    new ReportSpecification<CustomerFilter, CustomerVisitTracker>(DateTime.UtcNow, DateTime.UtcNow,
                        ReportResolution.Day, filter1, filter2);

                Assert.AreEqual(2, visitTrackerReportSpecification.FilterCombinations.Count());
                Assert.AreEqual(1, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
                Assert.AreEqual(visitTrackerReportSpecification.FilterCombinations.Count(fs => fs.Filters.Contains(
                    string.Format("ENVIRONMENT_SERVERNAME::ENV1,,GENDER::M,,STATE::CA,,STOREID::1234"))),1);
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