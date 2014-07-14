
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
using System.Threading;
using System.Threading.Tasks;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Publishing;
using Graphene.Tracking;
using Graphene.Attributes;
using Microsoft.VisualStudio.TestTools.UnitTesting;


namespace Graphene.Tests.Attributes
{
    [TrackerAttribute("Test Attribute", Description = "Test Tracker Description", ToolTip = "Test Tool Tip")]
    public class TestClass
    {
        [Measurement("Test Measument", Description = "Test Measurement Description", ToolTip = "Test Tool Tip")]
        public int Count{get; set;}
    }

    [TestClass]
    public class AttributeHelperExtentionsTest
    {
        [TestMethod]
        public void TestAttributeHelperCanReadTrackerAttributeValue()
        {
            var description = typeof(TestClass).GetAttributeValue((TrackerAttribute ta) => ta.Description);
            Assert.AreEqual("Test Tracker Description", description);
        }

        [TestMethod]
        public void TestAttributeHelperCanReadMeasurementAttributeValue()
        {
            var description = typeof(TestClass).GetAttributeValue((TestClass tc) => tc.Count, (MeasurementAttribute ma) => ma.Description);
            Assert.AreEqual("Test Measurement Description", description);
        }
    }
}
