// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Deployment.Internal;
using System.Configuration;
using Graphene.Configuration;
using Graphene.Publishing;
using Graphene.Reporting;

namespace Graphene.Configuration
{
    public enum TimespanRoundingMethod
    {
        MidPoint = 0,
        Start = 1,
        End = 2
    }
    public class Settings
    {
        internal bool Initialized { get; set; }
        public IPersist Persister { internal get; set; }
        public ILogger Logger { internal get; set; }
        public IReportGenerator ReportGenerator { internal get; set; }
        public Func<DateTime> EvaluateDateTime { get; set; }
        public TimespanRoundingMethod GrapheneRoundingMethod { internal get; set; }
        public TimeSpan DayTotalTZOffset { internal get; set; }
    }
}

namespace Graphene
{
    public class Configurator
    {
        private static Settings _configurator;
        
        internal static Settings Configuration
        {
            get { return _configurator; }
        }

        public static void Initialize(Settings configuration)
        {
            if (configuration == null || configuration.Persister == null)
            {
                throw new ArgumentNullException("Configuration and Configuration.Persister cannot be null");
            }
            configuration.Initialized = true;
            if (configuration.Logger == null)
                configuration.Logger = new SysDiagLogger();
            _configurator = configuration;
            TimespanRoundingMethod roundMethod;
            Enum.TryParse(ConfigurationManager.AppSettings["RoundingMethod"],true, out roundMethod);
            if (Enum.IsDefined(typeof (TimespanRoundingMethod), roundMethod))
            {
                _configurator.GrapheneRoundingMethod = roundMethod;
            }
            int offsetHours;
            int.TryParse(ConfigurationManager.AppSettings["MidnightOffsetForTotals"], out offsetHours);
            if (offsetHours >= -11 && offsetHours <= 14)
            {
                _configurator.DayTotalTZOffset = new TimeSpan(offsetHours, 0, 0);
            }
            else
            {
                _configurator.DayTotalTZOffset = new TimeSpan(0,0,0);
            }
            _configurator.Logger.Debug("Graphene Initialized");
            if (_configurator.EvaluateDateTime == null)
                _configurator.EvaluateDateTime = () => DateTime.SpecifyKind(DateTime.UtcNow,DateTimeKind.Utc);
        }

        public static bool FlushTrackers()
        {
            try
            {
                Publisher.FlushTrackers();
            }
            catch (Exception ex)
            {
                return false;
            }
            return true;

        }

        public static void ShutDown()
        {
            ShutDown(30);
        }

        public static void ShutDown(int timeoutInSeconds)
        {
            try
            {
                Publisher.ShutDown(timeoutInSeconds);
            }
            catch (Exception ex)
            {
                Configuration.Logger.Error(ex.Message, ex);
            }
        }

        public static TimeSpan DayTotalTZOffset()
        {
            return Configuration.DayTotalTZOffset;
        }
    }
}