using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Graphene.Reporting;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace Graphene.Tests.Reporting
{
    [TestClass]
    public class ReportSpecificationTests
    {
        [TestMethod]
        public void FirstTest()
        {
            var specification = new ReportSpecification(new[] {typeof (CustomerAgeTracker)},
                DateTime.UtcNow.AddDays(-100), DateTime.UtcNow, ReportResolution.Hour);

            Assert.IsTrue(specification.Counters.Any());


            string serializedObject = JsonConvert.SerializeObject(specification, Formatting.Indented);

            Console.WriteLine(serializedObject);
        }

        [TestMethod]
        public void GivenAFilterListWithEmptyFilters_WhenBuildingAReportSpecification_EmptyObjectValuesAreIgnored()
        {
            List<object> filtersWithTwoEmptyObjects = new List<object> { new FilteringStringFake(null,string.Empty), new FilteringStringFake("A","B"), new FilteringStringFake(null,null) };

            var specification = new ReportSpecification(new[] {typeof (CustomerAgeTracker)}, filtersWithTwoEmptyObjects,
                DateTime.UtcNow, DateTime.UtcNow.AddDays(-100), ReportResolution.Day);
            Assert.AreEqual(1, specification.FilterCombinations.Count());

        }

        public class FilteringStringFake
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            public FilteringStringFake(string aStringValue, string aSecondStringValue)
            {
                AStringValue = aStringValue;
                ASecondStringValue = aSecondStringValue;
            }

            public string AStringValue { get; set; }

            public string ASecondStringValue { get; set; }
        }
    }
}