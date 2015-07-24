﻿using System;
using System.Collections.Generic;
using System.Linq;
using Graphene.Configuration;
using Graphene.Reporting;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Newtonsoft.Json.Linq;

namespace Graphene.Mongo.Reporting
{
    public class MongoReportGenerator : IReportGenerator
    {
        private const string COLLECTION_NAME = "TrackerData";
        private static string _connectionString;
        private readonly ILogger _logger;

        private readonly Lazy<MongoCollection> _mongoCollection =
            new Lazy<MongoCollection>(() => initializeMongoCollection());

        #region Initialization

        public MongoReportGenerator(string connectionString, ILogger logger)
        {
            _connectionString = connectionString;
            _logger = logger;
        }

        private static MongoCollection initializeMongoCollection()
        {
            MongoServer mongoServer = new MongoClient(_connectionString).GetServer();
            string databaseName = MongoUrl.Create(_connectionString).DatabaseName;
            MongoDatabase mongoDatabase = mongoServer.GetDatabase(databaseName);
            return mongoDatabase.GetCollection(COLLECTION_NAME);
        }

        #endregion inittialization

        #region public methods

        public ITrackerReportResults BuildReport(IReportSpecification specification)
        {
            BsonDocument match = buildMatchCondition(specification);
            BsonDocument projection = buildProjection(specification);
            BsonDocument group = buildResultGroup(specification);
            var pipeline = new[] {match, projection, group};
            AggregateResult queryResults = _mongoCollection.Value.Aggregate(pipeline);

            if (!(queryResults.Ok && queryResults.ResultDocuments.Any()))
                _logger.Debug(string.Format("No Results Returned for Aggregation Pipeline: {0} {1}", pipeline.ToJson(),
                    System.Environment.NewLine));
            else
                _logger.Debug(string.Format("Results Returned for Aggregation Pipeline: {0} {1}", pipeline.ToJson(),
                    System.Environment.NewLine));

            return ToMongoAggregationResult(specification, queryResults);
        }

        #endregion public methods

        #region Static Mongo Aggregation Builders

        private static MongoAggregationResult ToMongoAggregationResult(object mr)
        {
            var jO = (JObject) mr;

            JToken dateTime = jO["UtcDateTime"];
            int year = dateTime["year"] != null ? Convert.ToInt32(dateTime["year"]) : DateTime.MinValue.Year;
            int month = dateTime["month"] != null ? Convert.ToInt32(dateTime["month"]) : 1;
            int day = dateTime["day"] != null ? Convert.ToInt32(dateTime["day"]) : 1;
            int hour = dateTime["hour"] != null ? Convert.ToInt32(dateTime["hour"]) : 0;
            int minute = dateTime["minute"] != null ? Convert.ToInt32(dateTime["minute"]) : 0;
            var measureTime = new DateTime(year, month, day, hour, minute, 0);

            var mongoRecord = new MongoAggregationResult(measureTime);

            foreach (var attr in jO)
            {
                if (attr.Key != "_id" || attr.Key != "UtcDateTime")
                {
                    long value = 0;
                    if (long.TryParse(attr.Value.ToString(), out value))
                    {
                        mongoRecord.MeasurementValues.Add(attr.Key, value);
                    }
                }
            }
            return mongoRecord;
        }

        private static ITrackerReportResults ToMongoAggregationResult(IReportSpecification specification,
            AggregateResult queryResults)
        {
            const string utcDateKey = "UtcDateTime";
            const string totalKey = "_Total";
            const string occurrenceKey = "_Occurrence";
            const string KEY_FILTER = "KeyFilter";
            const string ID_KEY = "_id";
            const string TYPE_KEY = "Type";

            var results = new MongoTrackerResults(specification);

            if (! queryResults.ResultDocuments.Any())
                return results;

            int count = queryResults.ResultDocuments.Count();
            List<string> names =
                queryResults.ResultDocuments.First().Names.Where(x => !(x == utcDateKey || x == "_id")).ToList();

            foreach (BsonDocument document in queryResults.ResultDocuments)
            {
                BsonDocument dateTime = document[utcDateKey].AsBsonDocument;

                long total = document[totalKey].ToInt64();
                long occurrence = document[occurrenceKey].ToInt64();
                string typeName = document[ID_KEY][TYPE_KEY].AsString;
                string keyFilter = document[ID_KEY][KEY_FILTER].ToString();
                DateTime utcDateTime = ConvertDateTimeDocumentToDateTime(dateTime);

                IAggregationBuildableResult trackerResult = results.AddAggregationResult(utcDateTime, typeName, keyFilter, occurrence, total);
                var relevantNames = names.Where(x => x.GetFullyQualifiedNameFromFormattedString().StartsWith(typeName));
                foreach (string key in relevantNames )
                {
                    string fullyQualifiedName = key.GetFullyQualifiedNameFromFormattedString();
                    BsonValue measurementResult = document[key];
                    IMeasurement measurement =
                        specification.Counters.FirstOrDefault(x => x.FullyQualifiedPropertyName == fullyQualifiedName);
                    if (measurement != null)
                        trackerResult.AddMeasurementResult(measurement, measurementResult.ToString());
                }
            }

            return results;
        }

        #endregion  Static Mongo Aggregation Builders

        #region Aggregation Query Builders

        private static BsonDocument buildResultGroup(IReportSpecification specification)
        {
            var timeElement = new BsonElement("Time", "$Time");
            var typeElement = new BsonElement("Type", "$Type");
            var keyfltr = new BsonElement("KeyFilter", "$KeyFilter");

            var idDocument = new BsonDocument(new List<BsonElement>() { timeElement, typeElement, keyfltr});
            var elements = new List<BsonElement> {new BsonElement("_id", idDocument)};
            elements.AddRange(specification.Counters.Select(
                counter => new BsonElement(counter.FormatFieldName(), new BsonDocument
                {
                    {"$sum", string.Format("$Measurement.{0}", counter.FullyQualifiedPropertyName)}
                })));

            elements.AddRange(new[]
            {
                new BsonElement("_Occurrence", new BsonDocument
                {
                    {"$sum", "$Measurement._Occurrence"}
                }),
                new BsonElement("_Total", new BsonDocument
                {
                    {"$sum", "$Measurement._Total"}
                }),
                new BsonElement("UtcDateTime", new BsonDocument
                {
                    {"$last", "$Time"}
                })
            });

            var group = new BsonDocument
            {
                {
                    "$group",
                    new BsonDocument(elements)
                }
            };
            return @group;
        }


        private static BsonDocument buildMatchCondition(IReportSpecification specification)
        {         
            IMongoQuery orClause = createSearchClauseForAnyFilter(specification);
            IMongoQuery typeNameClause = createSearchClauseForAnyType(specification);
            // Query.EQ("TypeName", specification.TrackerTypeName);
            IMongoQuery dateClause = Query.And(Query.GTE("TimeSlot", specification.FromDateUtc),
                Query.LTE("TimeSlot", specification.ToDateUtc));


            var conditions = new BsonDocument(dateClause.ToBsonDocument());
            conditions.AddRange(typeNameClause.ToBsonDocument());
            if (orClause != null)
                conditions.AddRange(orClause.ToBsonDocument());
            var match = new BsonDocument
            {
                {
                    "$match", conditions
                }
            };
            return match;
        }

        private static IMongoQuery createFilteredOrClause(IReportSpecification specification)
        {
            if (specification.FilterCombinations.Count() == 0)
                return null;

            IEnumerable<IMongoQuery> orQueries = specification.FilterCombinations
                .Select(filter =>
                    Query.In("SearchFilters", new BsonArray(filter.Filters)));

            IMongoQuery orClause = Query.Or(orQueries);
            return orClause;
        }

        private static IMongoQuery createSearchClauseForAnyFilter(IReportSpecification specification)
        {
            List<IMongoQuery> orQueries = specification.FilterCombinations
                .Select(filter =>
                    Query.In("SearchFilters", new BsonArray(filter.Filters))).ToList();

            if (orQueries.Any())
            {
                IMongoQuery orClause = Query.Or(orQueries);
                return orClause;
            }
            return null;
        }


        private static IMongoQuery createSearchClauseForAnyType(IReportSpecification specification)
        {
            IMongoQuery orQueries = Query.In("TypeName", new BsonArray(specification.TypeNames));


            IMongoQuery orClause = Query.Or(orQueries);
            return orClause;
        }

        private static BsonDocument buildProjection(IReportSpecification specification)
        {
            var year = new BsonElement("year", new BsonDocument("$year", new BsonString("$TimeSlot")));
            var month = new BsonElement("month", new BsonDocument("$month", new BsonString("$TimeSlot")));
            var day = new BsonElement("day", new BsonDocument("$dayOfMonth", new BsonString("$TimeSlot")));
            var hour = new BsonElement("hour", new BsonDocument("$hour", new BsonString("$TimeSlot")));
            var minute = new BsonElement("minute", new BsonDocument("$minute", new BsonString("$TimeSlot")));

            List<BsonElement> timeParts;
            switch (specification.Resolution)
            {
                case ReportResolution.Year:
                    timeParts = new List<BsonElement> {year};
                    break;
                case ReportResolution.Month:
                    timeParts = new List<BsonElement> {year, month};
                    break;
                case ReportResolution.Day:
                    timeParts = new List<BsonElement> {year, month, day};
                    break;
                case ReportResolution.Hour:
                    timeParts = new List<BsonElement> {year, month, day, hour};
                    break;
                default:
                    timeParts = new List<BsonElement> {year, month, day, hour, minute};
                    break;
            }

            var time = new BsonElement("Time", new BsonDocument(timeParts));
            var type = new BsonElement("Type", "$TypeName");
            var keyfltr = new BsonElement("KeyFilter", "$KeyFilter");
            var measurements = new List<BsonElement>();
            measurements.AddRange(new[]
            {
                new BsonElement("Measurement._Occurrence", "$Measurement._Occurrence"),
                new BsonElement("Measurement._Total", "$Measurement._Total")
            });
            measurements.AddRange(specification.Counters.Select(
                counter =>
                    new BsonElement(
                        string.Format("Measurement.{0}",
                            counter.FullyQualifiedPropertyName.GetFullyQualifiedNameFromFormattedString()),
                        string.Format("$Measurement.{0}", counter.PropertyName))));

            var projectElements = new List<BsonElement>();
            projectElements.Add(time);
            projectElements.Add(type);
            projectElements.Add(keyfltr);
            projectElements.AddRange(measurements);

            var projection = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument(projectElements)
                }
            };
            return projection;
        }


        private static DateTime ConvertDateTimeDocumentToDateTime(BsonDocument dateTime)
        {
            if (!dateTime.Contains("year"))
                throw new ArgumentException("Cannot convert document provided to basic datetime");

            int year = dateTime.Contains("year") ? Convert.ToInt32(dateTime["year"]) : DateTime.MinValue.Year;
            int month = dateTime.Contains("month") ? Convert.ToInt32(dateTime["month"]) : 1;
            int day = dateTime.Contains("day") ? Convert.ToInt32(dateTime["day"]) : 1;
            int hour = dateTime.Contains("hour") ? Convert.ToInt32(dateTime["hour"]) : 0;
            int minute = dateTime.Contains("minute") ? Convert.ToInt32(dateTime["minute"]) : 0;
            return new DateTime(year, month, day, hour, minute, 0);
        }

        #endregion Aggregation Query Builders

        private class MongoTrackerResults : ITrackerReportResultsBuilder
        {
            private readonly List<IAggregationResult> _aggregationResults = new List<IAggregationResult>();
            private readonly DateTime _fromDateUtc;
            private readonly ReportResolution _resolution;
            private readonly DateTime _toDateUtc;

            public MongoTrackerResults(IReportSpecification specification)
            {
                _fromDateUtc = specification.FromDateUtc;
                _toDateUtc = specification.ToDateUtc;
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
                get { return _aggregationResults; }
            }

            public ReportResolution resolution
            {
                get { return _resolution; }
            }


            public IAggregationBuildableResult AddAggregationResult(DateTime? mesurementTimeUtc, string typeName, string keyFilter, long occurence, long total)
            {
                var aggregationResultToReturn = new MongoTrackingAggregationResult(mesurementTimeUtc, typeName, keyFilter, occurence, total);
                _aggregationResults.Add(aggregationResultToReturn);
                return aggregationResultToReturn;
            }

            private class MongoTrackingAggregationResult : IAggregationBuildableResult
            {
                private readonly List<IMeasurementResult> _measurementValues;
                private DateTime? _mesurementTimeUtc;

                /// <summary>
                ///     Initializes a new instance of the <see cref="T:System.Object" /> class.
                /// </summary>
                public MongoTrackingAggregationResult(DateTime? mesurementTimeUtc, string typeName, string keyFilter, long occurence, long total)
                {
                    _mesurementTimeUtc = mesurementTimeUtc;
                    TypeName = typeName;
                    Occurence = occurence;
                    Total = total;
                    setKeyFilterDictionary(keyFilter);
                    _measurementValues = new List<IMeasurementResult>();
                }

                public IMeasurementResult AddMeasurementResult(IMeasurement measurement, string value)
                {
                    var resultToReturn = new MongoMeasurementResult(measurement, value);
                    _measurementValues.Add(resultToReturn);
                    return resultToReturn;
                }

                public ushort TimeSlice { get; set; }
                
                public string TypeName { get; set; }

                public Dictionary<string, string> KeyFilters { get; private set; }

                public DateTime MesurementTimeUtc
                {
                    get { return _mesurementTimeUtc.GetValueOrDefault(); }
                    set { _mesurementTimeUtc = value; }
                }

                public IEnumerable<IMeasurementResult> MeasurementValues
                {
                    get { return _measurementValues; }
                }

                public long Occurence { get; set; }

                public long Total { get; set; }

                private void setKeyFilterDictionary(string keyFilterStr)
                {
                    KeyFilters = new Dictionary<string, string>();
                    if (string.IsNullOrWhiteSpace(keyFilterStr)) return;
                    var keyFilterStrArray = keyFilterStr.Split(',');
                    if (!keyFilterStrArray.Any()) return;
                    foreach (var keyFilterArray in keyFilterStrArray.Where(keyFilter => !string.IsNullOrWhiteSpace(keyFilter))
                                                                    .Select(keyFilter => keyFilter.Split(new[] {"::"}, StringSplitOptions.None))
                                                                    .Where(keyFilterArray => keyFilterArray.Count() == 2))
                    {
                        KeyFilters.Add(keyFilterArray[0].Trim(), keyFilterArray[1].Trim());
                    }
                }

                private class MongoMeasurementResult : IMeasurementResult
                {
                    public MongoMeasurementResult(IMeasurement inboundMeasurement, string value)
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

                    public string Value { get; private set; }
                }
            }
        }
    }

    public class MongoAggregationResult : IQueryResults
    {
        public MongoAggregationResult(DateTime measurementTimeUtc)
        {
            MesurementTimeUtc = measurementTimeUtc;
            MeasurementValues = new Dictionary<string, long>();
        }

        public DateTime MesurementTimeUtc { get; private set; }

        public Dictionary<string, long> MeasurementValues { get; private set; }
    }
}

/*
private static BsonDocument buildProjectionOrig(IReportSpecification specification)
        {
            var year = new BsonElement("year", new BsonDocument("$year", new BsonString("$TimeSlot")));
            var month = new BsonElement("month", new BsonDocument("$month", new BsonString("$TimeSlot")));
            var day = new BsonElement("day", new BsonDocument("$dayOfMonth", new BsonString("$TimeSlot")));
            var hour = new BsonElement("hour", new BsonDocument("$hour", new BsonString("$TimeSlot")));
            var minute = new BsonElement("minute", new BsonDocument("$minute", new BsonString("$TimeSlot")));

            List<BsonElement> timeParts;
            switch (specification.Resolution)
            {
                case ReportResolution.Year:
                    timeParts = new List<BsonElement> {year};
                    break;
                case ReportResolution.Month:
                    timeParts = new List<BsonElement> {year, month};
                    break;
                case ReportResolution.Day:
                    timeParts = new List<BsonElement> {year, month, day};
                    break;
                case ReportResolution.Hour:
                    timeParts = new List<BsonElement> {year, month, day, hour};
                    break;
                default:
                    timeParts = new List<BsonElement> {year, month, day, hour, minute};
                    break;
            }

            var time = new BsonElement("Time", new BsonDocument(timeParts));
            var typeElement = new BsonElement("Type", "$TypeName");

            var measurements = new List<BsonElement>();
            measurements.AddRange(new[]
            {
                new BsonElement("Measurement._Occurrence", "$Measurement._Occurrence"),
                new BsonElement("Measurement._Total", "$Measurement._Total")
            });
            measurements.AddRange(specification.Counters.Select(
                counter =>
                    new BsonElement(string.Format("Measurement.{0}", counter.PropertyName),
                        string.Format("$Measurement.{0}", counter.PropertyName))));

            var projectElements = new List<BsonElement>();

            projectElements.Add(typeElement);
            projectElements.Add(time);
            projectElements.AddRange(measurements);

            var projection = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument(projectElements)
                }
            };
            return projection;
        }

        private static BsonDocument buildMatchConditionOrig(IReportSpecification specification)
        {
            IMongoQuery orClause = null; //createFilteredOrClause(specification);
            IMongoQuery typeNameClause = Query.EQ("TypeName", specification.Counters.First().TrackerTypeName);
            IMongoQuery dateClause = Query.And(Query.GTE("TimeSlot", specification.FromDateTime),
                Query.LTE("TimeSlot", specification.ToDateTime));

            var conditions = new BsonDocument(dateClause.ToBsonDocument());
            conditions.AddRange(typeNameClause.ToBsonDocument());
            if (orClause != null)
                conditions.AddRange(orClause.ToBsonDocument());
            var match = new BsonDocument
            {
                {
                    "$match", conditions
                }
            };
            return match;
        }
   private static BsonDocument buildResultGroupOrig(IReportSpecification specification)
        {
            var elements = new List<BsonElement> {new BsonElement("_id", new BsonString("$Time"))};
            elements.AddRange(specification.Counters.Select(
                counter => new BsonElement(counter.PropertyName, new BsonDocument
                {
                    {"$sum", string.Format("$Measurement.{0}", counter.PropertyName)}
                })));

            elements.AddRange(new[]
            {
                new BsonElement("_Occurrence", new BsonDocument
                {
                    {"$sum", "$Measurement._Occurrence"}
                }),
                new BsonElement("_Total", new BsonDocument
                {
                    {"$sum", "$Measurement._Total"}
                }),
                new BsonElement("UtcDateTime", new BsonDocument
                {
                    {"$last", "$Time"}
                })
            });

            var group = new BsonDocument
            {
                {
                    "$group",
                    new BsonDocument(elements)
                }
            };
            return @group;
        }* 
 * 
 * 
 * 

 * 
 * 
*/