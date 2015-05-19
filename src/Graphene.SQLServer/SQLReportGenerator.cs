// Copyright 2013-2014 Boban Jose
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. 
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, 
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Reporting;

namespace Graphene.SQLServer
{
    public class SQLReportGenerator : IReportGenerator
    {
        private readonly string _connectionString;
        private readonly ILogger _logger;

        public SQLReportGenerator(string connectionString, ILogger logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        public ITrackerReportResults BuildReport(IReportSpecification specification)
        {
            try
            {
                var results = new SqlTrackerResults(specification);

                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (var command = connection.CreateCommand())
                    {
                        if (specification.OffsetTotalsByHours != TimeSpan.Zero && specification.OffsetTotalsByHours.Hours != Configurator.DayTotalTZOffset().Hours)
                        {
                            command.CommandText = "dbo.GenerateReportWithOffsetTotals";
                            command.Parameters.Add("@OffsetTotalsTo", SqlDbType.SmallInt);
                            command.Parameters["@OffsetTotalsTo"].Value = specification.OffsetTotalsByHours.Hours;
                        }
                        else
                        {
                            command.CommandText = "dbo.GenerateReport";
                        }
                        command.CommandType = CommandType.StoredProcedure;

                        command.Parameters.Add("@StartDt", SqlDbType.DateTime);
                        command.Parameters["@StartDt"].Value = specification.FromDateTime;

                        command.Parameters.Add("@EndDt", SqlDbType.DateTime);
                        command.Parameters["@EndDt"].Value = specification.ToDateTime;

                        command.Parameters.Add("@Resolution", SqlDbType.SmallInt);
                        command.Parameters["@Resolution"].Value = (int)specification.Resolution;

                        SqlParameter flParameter;
                        flParameter = command.Parameters.AddWithValue("@FilterList", createFilterDataTable(specification));
                        flParameter.SqlDbType = SqlDbType.Structured;
                        flParameter.TypeName = "dbo.FilterList";

                        SqlParameter tnParameter;
                        tnParameter = command.Parameters.AddWithValue("@TypeNameList", createTypeNameDataTable(specification));
                        tnParameter.SqlDbType = SqlDbType.Structured;
                        tnParameter.TypeName = "dbo.TypeName";

                        var sqlReader = command.ExecuteReader();

                        string query = command.CommandText;

                        foreach (SqlParameter p in command.Parameters)
                        {
                            query = query.Replace(p.ParameterName, p.Value.ToString());
                        }

                        int timeSlotC = sqlReader.GetOrdinal("MeasureTime");

                        var mesurementTypeList = new Dictionary<string, IMeasurement>();

                        while (sqlReader.Read())
                        {
                            DateTime? measurementDate = null;

                            if (!sqlReader.IsDBNull(timeSlotC))
                            {
                                measurementDate = sqlReader.GetDateTime(timeSlotC);

                                string typeName = null;
                                if (sqlReader["TypeName"] != DBNull.Value)
                                    typeName = (string) sqlReader["TypeName"];

                                string measurementName = null;
                                if (sqlReader["MeasurementName"] != DBNull.Value)
                                    measurementName = (string) sqlReader["MeasurementName"];

                                IMeasurement measurement = null;
                                string typeFullyName = null;

                                if (typeName != null && measurementName != null)
                                {
                                    typeFullyName = string.Concat(typeName, ".", measurementName);
                                    mesurementTypeList.TryGetValue(typeFullyName, out measurement);
                                }

                                if (measurement == null)
                                {
                                    if (typeFullyName != null)
                                    {
                                        measurement = specification.Counters.FirstOrDefault(
                                            x => x.FullyQualifiedPropertyName == typeFullyName) ??
                                                      new SqlMeasurement()
                                                      {
                                                          TrackerTypeName = typeName,
                                                          Description = typeName,
                                                          DisplayName = typeName,
                                                          PropertyName = measurementName
                                                      };
                                        mesurementTypeList.Add(typeFullyName, measurement);
                                    }
                                    else
                                    {
                                        measurement = new SqlMeasurement()
                                        {
                                            TrackerTypeName = typeName,
                                            Description = typeName,
                                            DisplayName = typeName,
                                            PropertyName = measurementName
                                        };
                                    }
                                }
                                results.AddAggregationResult(measurementDate, typeName,
                                    (sqlReader.HasColumn("Filter") && sqlReader["Filter"] != DBNull.Value)
                                        ? (string) sqlReader["Filter"]
                                        : null,
                                    measurement, (long) sqlReader["MeasurementValue"]);
                            }
                        }
                    }
                }
                return results;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message, ex);
                throw;
            }
        }

        private class SqlTrackerResults : ITrackerReportResultsBuilder
        {
            private readonly Dictionary<string, SqlTrackingAggregationResult> _aggregationResults = new Dictionary<string, SqlTrackingAggregationResult>();
            private readonly DateTime _fromDateUtc;
            private readonly ReportResolution _resolution;
            private readonly DateTime _toDateUtc;

            public SqlTrackerResults(IReportSpecification specification)
            {
                _fromDateUtc = specification.FromDateTime;
                _toDateUtc = specification.ToDateTime;
                _resolution = specification.Resolution;
            }

            public DateTime FromDateUtc
            {
                get { return _fromDateUtc; }
            }

            public DateTime ToDateUtc
            {
                get { return _toDateUtc; }
            }

            public IEnumerable<IAggregationResult> AggregationResults
            {
                get { return _aggregationResults.Values; }
            }

            public ReportResolution resolution
            {
                get { return _resolution; }
            }

            public IAggregationBuildableResult AddAggregationResult(DateTime? mesurementTimeUtc, string typeName, string keyFilter,
                long occurence, long total)
            {
                throw new NotImplementedException();
            }

            public IAggregationBuildableResult AddAggregationResult(DateTime? mesurementTimeUtc, string typeName, string filter,
                IMeasurement measurement, long measurementValue)
            {
                SqlTrackingAggregationResult aggregationResult = null;

                var key = string.Concat(typeName, filter, mesurementTimeUtc.GetValueOrDefault());

                _aggregationResults.TryGetValue(key, out aggregationResult);

                if (aggregationResult == null)
                {
                    aggregationResult = new SqlTrackingAggregationResult(mesurementTimeUtc, typeName, filter);
                    _aggregationResults.Add(key, aggregationResult);
                }
                if (measurement.PropertyName == "_Total")
                    aggregationResult.Total = measurementValue;
                else if (measurement.PropertyName == "_Occurrence")
                    aggregationResult.Occurence = measurementValue;
                else
                    aggregationResult.AddMeasurementResult(measurement, measurementValue);
                return aggregationResult;
            }

            private class SqlTrackingAggregationResult : IAggregationBuildableResult
            {
                private readonly List<IMeasurementResult> _measurementValues;
                private DateTime? _mesurementTimeUtc;

                /// <summary>
                ///     Initializes a new instance of the <see cref="T:System.Object" /> class.
                /// </summary>
                public SqlTrackingAggregationResult(DateTime? mesurementTimeUtc, string typeName, string filter)
                {
                    _mesurementTimeUtc = mesurementTimeUtc;
                    TypeName = typeName;
                    setFilterDictionary(filter);
                    _measurementValues = new List<IMeasurementResult>();
                }

                public IMeasurementResult AddMeasurementResult(IMeasurement measurement, long value)
                {
                    var resultToReturn = new SqlMeasurementResult(measurement, value);
                    _measurementValues.Add(resultToReturn);
                    return resultToReturn;
                }

                public ushort TimeSlice { get; set; }

                public string TypeName { get; set; }

                public Dictionary<string, string> KeyFilters { get; private set; }

                public DateTime? MesurementTimeUtc
                {
                    get { return _mesurementTimeUtc; }
                    set { _mesurementTimeUtc = value; }
                }

                public IEnumerable<IMeasurementResult> MeasurementValues
                {
                    get { return _measurementValues; }
                }

                public long Occurence { get; set; }

                public long Total { get; set; }
                public IMeasurementResult AddMeasurementResult(IMeasurement measurement, string value)
                {
                    throw new NotImplementedException();
                }

                private void setFilterDictionary(string filterStr)
                {
                    KeyFilters = new Dictionary<string, string>();
                    if (string.IsNullOrWhiteSpace(filterStr)) return;
                    var filterStrArray = filterStr.Split(',');
                    if (!filterStrArray.Any()) return;
                    foreach (var filterArray in filterStrArray.Where(filter => !string.IsNullOrWhiteSpace(filter))
                                                                    .Select(filter => filter.Split(new[] { "::" }, StringSplitOptions.None))
                                                                    .Where(keyFilterArray => keyFilterArray.Count() == 2))
                    {
                        KeyFilters.Add(filterArray[0].Trim(), filterArray[1].Trim());
                    }
                }
            }
        }

        private DataTable createFilterDataTable(IReportSpecification specifications)
        {
            var table = new DataTable();
            table.Columns.Add("Filter", typeof (string));
            foreach (var filter in specifications.FilterCombinations)
            {
                if (filter.Filters != null && filter.Filters.Any())
                    table.Rows.Add(filter.Filters.First());
            }
            return table;
        }

        private DataTable createTypeNameDataTable(IReportSpecification specifications)
        {
            var table = new DataTable();
            table.Columns.Add("TypeName", typeof(string));
            foreach (var tn in specifications.TypeNames)
            {
                table.Rows.Add(tn);
            }
            return table;
        }

        public class SqlMeasurementResult : IMeasurementResult
        {
            public SqlMeasurementResult(IMeasurement inboundMeasurement, long value)
            {
                Value = value;
                PropertyName = inboundMeasurement.PropertyName;
                DisplayName = inboundMeasurement.DisplayName;
                Description = inboundMeasurement.Description;
                TrackerTypeName = inboundMeasurement.TrackerTypeName;
                FullyQualifiedPropertyName = string.Format("{0}.{1}", TrackerTypeName, PropertyName);
            }

            public string PropertyName { get; private set; }

            public string TrackerTypeName { get; private set; }

            public string DisplayName { get; private set; }

            public string Description { get; private set; }

            public string FullyQualifiedPropertyName { get; private set; }

            public long Value { get; private set; }
        }

        public class SqlMeasurement : IMeasurement
        {
            public string PropertyName { get; set; }
            public string TrackerTypeName { get; set; }
            public string DisplayName { get; set; }
            public string Description { get; set; }
            public string FullyQualifiedPropertyName { get; set; }
        }
    }

    public static class DataRecordExtensions
    {
        public static bool HasColumn(this IDataRecord dr, string columnName)
        {
            for (int i = 0; i < dr.FieldCount; i++)
            {
                if (dr.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
                    return true;
            }
            return false;
        }
    }
}
