// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Publishing;
using Graphene.Tracking;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;

namespace Graphene.SQLServer
{
    public class PersistToSQLServer : IPersist
    {
        private readonly string _connectionString;
        private readonly ILogger _logger;
        private readonly bool _convertToUTC;
        private readonly int _maxRetries;
        private readonly int _initialRetry;
        private readonly int _incrementalRetry;
        private static bool _persistPreAggregatedBuckets;

        public PersistToSQLServer(string connectionString, ILogger logger, bool convertToUTC = false, bool persistPreAggregatedBuckets = false, int maxRetries = 5, int initialRetry = 200, int incrementalRetry = 300)
        {
            _connectionString = connectionString;
            _logger = logger;
            _persistPreAggregatedBuckets = PersistPreAggregatedBuckets = persistPreAggregatedBuckets;
            _convertToUTC = convertToUTC;
            _maxRetries = maxRetries;
            _initialRetry = initialRetry;
            _incrementalRetry = incrementalRetry;
        }

        public void Persist(TrackerData trackerData)
        {
            try
            {
                Persist(trackerData, 0);
            }
            catch (Exception ex)
            {
                var message = new StringBuilder();
                message.AppendLine(ex.Message);
                message.AppendFormat("Tracker Data: TypeName: {0} KeyFilter: {1} TimeSlot: {2}", trackerData.TypeName, trackerData.KeyFilter, trackerData.TimeSlot);
                _logger.Error(message.ToString(), ex);
            }
        }

        private void Persist(TrackerData trackerData, int retryCount)
        {
            persitTracker(trackerData);
        }

        public bool PersistPreAggregatedBuckets { get; set; }

        private void persitTracker(TrackerData trackerData)
        {
            if (trackerData.MinResolution == Resolution.NA || trackerData.TimeSlot.Kind == DateTimeKind.Local)
                trackerData.TimeSlot = trackerData.TimeSlot.ToUniversalTime();

            var retryStrategy = new Incremental(_maxRetries, TimeSpan.FromMilliseconds(_initialRetry), TimeSpan.FromSeconds(_incrementalRetry));
            var retryPolicy = new RetryPolicy<SqlDatabaseTransientErrorDetectionStrategy>(retryStrategy);

            using (var connection = new SqlConnection(_connectionString))
            {
                connection.OpenWithRetry(retryPolicy);
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "dbo.UpdateTracker";
                    command.CommandType = CommandType.StoredProcedure;

                    command.Parameters.Add("@TrackerID", SqlDbType.NVarChar);
                    command.Parameters["@TrackerID"].Value = String.Format("{0}{1}",trackerData.TypeName, trackerData.KeyFilter).Replace(" ", "");

                    command.Parameters.Add("@Name", SqlDbType.NVarChar);
                    command.Parameters["@Name"].Value = trackerData.Name;

                    command.Parameters.Add("@TypeName", SqlDbType.NVarChar);
                    command.Parameters["@TypeName"].Value = trackerData.TypeName;

                    command.Parameters.Add("@MinResolution", SqlDbType.Int);
                    command.Parameters["@MinResolution"].Value = bucketResolutionToMinutes(trackerData.MinResolution);

                    command.Parameters.Add("@KeyFilter", SqlDbType.NVarChar);
                    command.Parameters["@KeyFilter"].Value = trackerData.KeyFilter;

                    command.Parameters.Add("@TimeSlot", SqlDbType.DateTime);
                    command.Parameters["@TimeSlot"].Value = _convertToUTC ? trackerData.TimeSlot.ToUniversalTime() : trackerData.TimeSlot;

                    SqlParameter flParameter;
                    flParameter = command.Parameters.AddWithValue("@FilterList", createFilterDataTable(trackerData));
                    flParameter.SqlDbType = SqlDbType.Structured;
                    flParameter.TypeName = "dbo.FilterList";

                    SqlParameter mParameter;
                    mParameter = command.Parameters.AddWithValue("@Measurement", createMeasurementDataTable(trackerData));
                    mParameter.SqlDbType = SqlDbType.Structured;
                    mParameter.TypeName = "dbo.Measurement";

                    using (var transaction = connection.BeginTransaction())
                    {
                        command.Transaction = transaction;
                        command.CommandTimeout = command.CommandTimeout * 2;
                        command.ExecuteNonQueryWithRetry(retryPolicy);
                        transaction.Commit();
                    }
                }
            }
        }

        private DataTable createFilterDataTable(TrackerData trackerData)
        {
            var table = new DataTable();
            table.Columns.Add("Filter", typeof(string));
            foreach (string filter in trackerData.SearchFilters)
            {
                table.Rows.Add(filter);
            }
            return table;
        }

        private static DataTable createMeasurementDataTable(TrackerData trackerData)
        {
            var table = new DataTable();
            table.Columns.Add("Name", typeof(string));
            table.Columns.Add("Value", typeof(long));
            table.Columns.Add("BucketResolution", typeof(int));
            table.Columns.Add("CoversMinuteBucket", typeof(bool));
            table.Columns.Add("CoversFiveMinuteBucket", typeof(bool));
            table.Columns.Add("CoversFifteenMinuteBucket", typeof(bool));
            table.Columns.Add("CoversThirtyMinuteBucket", typeof(bool));
            table.Columns.Add("CoversHourBucket", typeof(bool));
            table.Columns.Add("CoversDayBucket", typeof(bool));
            table.Columns.Add("CoversMonthBucket", typeof(bool));

            foreach (var metrics in trackerData.Measurement.NamedMetrics)
            {
                addToMeasurementTable(trackerData, table, bucketResolutionToMinutes(trackerData.Measurement.BucketResolution), metrics.Key, metrics.Value);
            }

            addToMeasurementTable(trackerData, table, bucketResolutionToMinutes(trackerData.Measurement.BucketResolution), "_Occurrence", trackerData.Measurement._Occurrence);
            addToMeasurementTable(trackerData, table, bucketResolutionToMinutes(trackerData.Measurement.BucketResolution), "_Total", trackerData.Measurement._Total);
            return table;
        }

        private static int bucketResolutionToMinutes(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.NA:
                    return -1;
                case Resolution.Minute:
                    return 1;
                case Resolution.FiveMinute:
                    return 5;
                case Resolution.FifteenMinute:
                    return 15;
                case Resolution.ThirtyMinute:
                    return 30;
                case Resolution.Hour:
                    return 60;
                case Resolution.Day:
                    return 1440;
                case Resolution.Month:
                    return 43200;
            }
            throw new Exception("Unsupported Resolution");
        }

        private static void addToMeasurementTable(TrackerData trackerData, DataTable table, int measurementResolutionMinutes, string metricName, long metricValue)
        {
            if (trackerData.Measurement.CoveredResolutions != null)
            {
                if (_persistPreAggregatedBuckets)
                {
                    table.Rows.Add(metricName, metricValue, measurementResolutionMinutes
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.Minute)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.FiveMinute)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.FifteenMinute)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.ThirtyMinute)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.Hour)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.Day)
                        , trackerData.Measurement.CoveredResolutions.Contains(Resolution.Month));
                }
                else
                {
                    table.Rows.Add(metricName, metricValue, measurementResolutionMinutes, 0, 0, 0, 0, 0, 0, 0);
                }
            }
        }
    }
}
