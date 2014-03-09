using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Graphene.Reporting;
using Graphene.Tracking;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Builders;
using Newtonsoft.Json;
using MongoDB.Bson.IO;
using Newtonsoft.Json.Linq;

namespace Graphene.Mongo.Reporting
{  

    public class MongoReportGenerator : IReportGenerator
    {
        private const string COLLECTION_NAME = "TrackerData";
        private static string _connectionString;
        
        private Lazy<MongoCollection> _mongoCollection = new Lazy<MongoCollection>(()=>initializeMongoCollection() );

        private static MongoCollection initializeMongoCollection()
        {
            var mongoServer = new MongoClient(_connectionString).GetServer();
            var databaseName = MongoUrl.Create(_connectionString).DatabaseName;
            var mongoDatabase = mongoServer.GetDatabase(databaseName);
            return  mongoDatabase.GetCollection(COLLECTION_NAME);
        }


        public MongoReportGenerator(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IEnumerable<IQueryResults> GeneratorReport(IReportSpecification specification)
        {
            var match = buildMatchCondition(specification);
            var projection = buildProjection(specification);
            var group = buildResultGroup(specification);


            var pipeline = new[] { match, projection, group };

            var queryResults = _mongoCollection.Value.Aggregate(pipeline);

            if (!(queryResults.Ok && queryResults.ResultDocuments.Any()))
                return null;

            return queryResults.ResultDocuments
                    .Select(rd => JsonConvert.DeserializeObject(rd.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict })))
                    .Select(ToMongoAggregationResult)
                    .ToList();
        }

        public MongoAggregationResult ToMongoAggregationResult(object mr)
        {
            var jO = (JObject)mr;

            var dateTime = jO["UtcDateTime"];
            var year = dateTime["year"]!=null ? Convert.ToInt32(dateTime["year"]) : DateTime.MinValue.Year;
            var month = dateTime["month"] != null ? Convert.ToInt32(dateTime["month"]) : 1;
            var day = dateTime["day"] != null ? Convert.ToInt32(dateTime["day"]) : 1;
            var hour = dateTime["hour"] != null ? Convert.ToInt32(dateTime["hour"]) : 0;
            var minute = dateTime["minute"] != null ? Convert.ToInt32(dateTime["minute"]) : 0;
            var measureTime = new DateTime(year, month, day, hour, minute, 0);

            var mongoRecord = new MongoAggregationResult(measureTime);

            foreach (var attr in jO)
            {
                if (attr.Key != "_id" || attr.Key != "UtcDateTime")
                {
                    long value = 0;
                    if(long.TryParse(attr.Value.ToString(), out value))
                    {
                        mongoRecord.MeasurementValues.Add(attr.Key, value);
                    }
                }
            }
            return mongoRecord;
        }

        public class MongoAggregationResult : IQueryResults
        {
            public MongoAggregationResult(DateTime measurementTimeUtc)
            {
                MesurementTimeUtc = measurementTimeUtc;
                MeasurementValues = new Dictionary<string, long>();
            }

            public DateTime MesurementTimeUtc
            {
                get;
                private set;
            }

            public Dictionary<string, long> MeasurementValues
            {
                get;
                private set;
            }
        }

        private static BsonDocument buildProjection(IReportSpecification specification)
        {
            var year =  new BsonElement( "year", new BsonDocument("$year", new BsonString("$TimeSlot")));
            var month = new BsonElement("month", new BsonDocument("$month", new BsonString("$TimeSlot")));
            var day = new BsonElement("day", new BsonDocument("$dayOfMonth", new BsonString("$TimeSlot")));
            var hour = new BsonElement("hour", new BsonDocument("$hour", new BsonString("$TimeSlot")));
            var minute = new BsonElement("minute", new BsonDocument("$minute", new BsonString("$TimeSlot")));

            List<BsonElement> timeParts;
            switch(specification.Resolution){
                case ReportResolution.Year:
                    timeParts = new List<BsonElement> { year };
                    break;
                case ReportResolution.Month:
                    timeParts = new List<BsonElement> { year, month };
                    break;
                case ReportResolution.Day:
                    timeParts = new List<BsonElement> { year, month, day };
                    break;
                case ReportResolution.Hour:
                    timeParts = new List<BsonElement> { year, month, day, hour };
                    break;
                default:
                    timeParts = new List<BsonElement> { year, month, day, hour, minute };
                    break;
            }
            
            var time = new BsonElement("Time", new BsonDocument(timeParts));

            var measurements = new List<BsonElement>();
            measurements.AddRange(new[] { new BsonElement("Measurement._Occurrence", "$Measurement._Occurrence"), new BsonElement("Measurement._Total", "$Measurement._Total") });
            measurements.AddRange(specification.Counters.Select(
                counter => new BsonElement(string.Format("Measurement.{0}", counter), string.Format("$Measurement.{0}", counter))));

            var projectElements = new List<BsonElement>();
            projectElements.Add(time);
            projectElements.AddRange(measurements);

            var projection = new BsonDocument { 
                   {
                            "$project",
                            new BsonDocument(projectElements)
                    }
            };
            return projection;
        }

        private static BsonDocument buildResultGroup(IReportSpecification specification)
        {

            var elements = new List<BsonElement> { new BsonElement("_id", new BsonString("$Time")) };
            elements.AddRange(specification.Counters.Select(
                counter => new BsonElement(counter, new BsonDocument()
            {
                {"$sum", string.Format("$Measurement.{0}", counter)}
            })));

            elements.AddRange(new[] { new BsonElement("_Occurrence",  new BsonDocument()
            {
                {"$sum", "$Measurement._Occurrence"}
            }),
            new BsonElement("_Total",  new BsonDocument()
            {
                {"$sum", "$Measurement._Total"}
            }),
            new BsonElement("UtcDateTime",  new BsonDocument()
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
            var orClause = createFilteredOrClause(specification);
            var typeNameClause = Query.EQ("TypeName", specification.TrackerTypeName);
            var dateClause = Query.And(Query.GTE("TimeSlot", specification.FromDateUtc),
                Query.LTE("TimeSlot", specification.ToDateUtc));           
            
            var conditions = new BsonDocument(dateClause.ToBsonDocument());
            conditions.Add(typeNameClause.ToBsonDocument());
            if(orClause!=null)
                conditions.Add(orClause.ToBsonDocument());
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

            var orQueries = specification.FilterCombinations
                .Select(filter =>
                    Query.In("SearchFilters", new BsonArray(filter.Filters)));                    

            var orClause = Query.Or(orQueries);
            return orClause;
        }
    }
}
