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
using MongoDB.Driver.Builders;

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




        public IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification)
        {
            var match = buildMatchCondition(specification);

            var @group = buildResultGroup(specification);


            var pipeline = new[] { match, @group };

            var queryResults = _mongoCollection.Value.Aggregate(pipeline);

            if(! (queryResults.Ok && queryResults.ResultDocuments.Any()))
                return null;

            var resultDocument = queryResults.ResultDocuments.ElementAt(0);
            var numberofValues = resultDocument.Names.Count();
            
            List<IAggregationResult> results = new List<IAggregationResult>(numberofValues-1);
            for (int i = 0; i < numberofValues; ++i)
            {
                
                var name = resultDocument.Names.ElementAt(i);
                if (name == "_id")
                    continue;
                var value = resultDocument.Values.ElementAt(i);
                results.Add( 
                    new MongoAggregationResult(name,value.ToString())
                    );

            }


            return results;

        }

        private class MongoAggregationResult : IAggregationResult
        {
            private string _measurementName;
            private string _measurementValue;

            /// <summary>
            /// Initializes a new instance of the <see cref="T:System.Object"/> class.
            /// </summary>
            public MongoAggregationResult(string measurementName, string measurementValue)
            {
                _measurementName = measurementName;
                _measurementValue = measurementValue;
            }

            public string MeasurementName
            {
                get { return _measurementName; }
                set { _measurementName = value; }
            }

            public string MeasurementValue
            {
                get { return _measurementValue; }
                set { _measurementValue = value; }
            }
        }

        private static BsonDocument buildResultGroup(IReportSpecification specification)
        {
            
            var elements = new List<BsonElement> {new BsonElement("_id", new BsonString("abc"))};
            elements.AddRange(specification.Counters.Select(
                counter => new BsonElement(counter, new BsonDocument()
            {
                {"$sum", string.Format("$Measurement.{0}", counter)}
            })));

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
            var orQueries = specification.FilterCombinations
                .Select(filter =>
                    Query.All("SearchFilters", new BsonArray(filter.Filters)));
                    

            var orClause = Query.Or(orQueries);
            return orClause;
        }
    }
}
