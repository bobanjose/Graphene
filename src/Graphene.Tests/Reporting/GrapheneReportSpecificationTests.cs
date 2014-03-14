using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
         
         GrapheneReportSpecification specification = new GrapheneReportSpecification( new Type[] {typeof(CustomerAgeTracker)}, DateTime.UtcNow.AddDays(-100), DateTime.UtcNow, ReportResolution.Hour);

         Assert.IsTrue(specification.Counters.Any());


         var serializedObject = JsonConvert.SerializeObject(specification,Formatting.Indented);

         Console.WriteLine(serializedObject);

     }

    }
}
