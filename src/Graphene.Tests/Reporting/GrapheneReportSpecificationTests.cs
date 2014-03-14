using System;
using System.Linq;
using Graphene.Reporting;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;

namespace Graphene.Tests.Reporting
{
    [TestClass]
    public class GrapheneReportSpecificationTests
    {
        [TestMethod]
        public void FirstTest()
        {
            var specification = new GrapheneReportSpecification(new[] {typeof (CustomerAgeTracker)},
                DateTime.UtcNow.AddDays(-100), DateTime.UtcNow, ReportResolution.Hour);

            Assert.IsTrue(specification.Counters.Any());


            string serializedObject = JsonConvert.SerializeObject(specification, Formatting.Indented);

            Console.WriteLine(serializedObject);
        }
    }
}