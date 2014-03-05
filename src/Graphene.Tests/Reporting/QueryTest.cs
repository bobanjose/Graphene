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

             ReportSpecification<CustomerVisitTracker> visitTrackerReportSpecification = new ReportSpecification<CustomerVisitTracker>(DateTime.Now,DateTime.UtcNow, filter1,filter2);

             Assert.AreEqual(2, visitTrackerReportSpecification.FilterCombinations.Count() );
             Assert.AreEqual(4, visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.Count());
             Assert.AreEqual(
                 string.Format("{0}::{1}",
                     ("Environment_ServerName").ToUpper(), filter1.Environment_ServerName.ToUpper()),
                 visitTrackerReportSpecification.FilterCombinations.ElementAt(0).Filters.ElementAt(0));
         }

         public void GivenAQueryWithTrackerProperties_WhenBuildingTheListOfTrackersToTrack_OnlyTheAppropriateCountersAreCounted()
         {

             var visitTrackerReportSpecification = new ReportSpecification<CustomerAgeTracker>(DateTime.Now, DateTime.UtcNow);

             Assert.AreEqual(3, visitTrackerReportSpecification.Counters.Count());

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