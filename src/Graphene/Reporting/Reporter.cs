using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Graphene.Tracking;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Graphene.Reporting
{
    using MongoDB.Driver.Builders;

    public interface IReportGenerator
    {
        IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification);

    }

    public class Reporter : IReportGenerator
    {
        private const string COLLECTION_NAME = "TrackerData";
        private static MongoServer _mongoServer;
        private static MongoDatabase _mongoDatabase;
        private static MongoCollection _mongoCollection;



        public Reporter(string connectionString)
        {
            _mongoServer = new MongoClient(connectionString).GetServer();
            var databaseName = MongoUrl.Create(connectionString).DatabaseName;
            _mongoDatabase = _mongoServer.GetDatabase(databaseName);
            _mongoCollection = _mongoDatabase.GetCollection(COLLECTION_NAME);

        }

        /*
         * 
         * 
         *  { $match : 
         *  {   TypeName :"CommonWell.Framework.Reporting.Trackers.NetworkLinksTracker" ,
         *  
            $or: 
         *  [ {  SearchFilters : { $all :  [ "VENDOR::ORANGE", "ORGID::CATALYST101" ] } }, 
         *  {  SearchFilters :  { $all :  [ "VENDOR::DRIVE" , "ORGID::2.16.840.1.113883.3.3330.757405907"] }} 
         *  ] }  
         *  },
            { 
         *  $group:{ _id: "Report",
            NetworkLinksValidated : { $sum : "$Measurement.NetworkLinksValidated" },
            NetworkLinkRequests: { $sum : "$Measurement.NetworkLinkRequests" },
            NetworkLinksReturned : { $sum : "$Measurement.NetworkLinksReturned" },
            NetworkLinksInvalidated : { $sum : "$Measurement.NetworkLinksInvalidated" },
            TotalRecords : { $sum : 1 } } }
         */

        public IEnumerable<IAggregationResult> GeneratorReport<TTracker>(DateTime fromDateUtc, DateTime toUtcDate, params object[] filters)
            where TTracker : ITrackable
        {
            List<BsonDocument> documents = new List<BsonDocument>();

            var container = new BsonDocument();
            return null;
        }

        public IEnumerable<IAggregationResult> GeneratorReport(Type typeofTracker, DateTime fromDateUtc, DateTime toUtcDate, params object[] filters)
        {
            return null;
        } 


        public IEnumerable<IAggregationResult> GeneratorReport(IReportSpecification specification)
        {
            return null;
        }
    }


}

