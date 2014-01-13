// Copyright 2013-2014 Boban Jose
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Graphene.Data;
using MongoDB;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver.Builders;


namespace Graphene.Publishing
{
    public class PersistToMongo: IPersist
    {
        private const string COLLECTION_NAME = "TrackerData";
        private static MongoServer _mongoServer;
        private static MongoDatabase _mongoDatabase;
        private static MongoCollection _mongoCollection;

        public PersistToMongo(string connectionString)
        {
            _mongoServer = new MongoClient(connectionString).GetServer();
            var databaseName = MongoUrl.Create(connectionString).DatabaseName;
            _mongoDatabase = _mongoServer.GetDatabase(databaseName);
            _mongoCollection = _mongoDatabase.GetCollection(COLLECTION_NAME);          

        }

        public void Persist(TrackerData trackerData)
        {
            try
            {
                var update = Update<TrackerData>.Inc(e => e.Measurement.Total, trackerData.Measurement.Total)
                                                .Inc(e => e.Measurement.Occurrence, trackerData.Measurement.Occurrence)
                                                .SetOnInsert(e => e.TypeName, trackerData.TypeName)                                                
                                                .SetOnInsert(e => e.KeyFilter, trackerData.KeyFilter)                                                
                                                .SetOnInsert(e => e.Name, trackerData.Name)
                                                .SetOnInsert(e => e.SearchFilters, trackerData.SearchFilters)
                                                .SetOnInsert(e => e.TimeSlot, trackerData.TimeSlot);                                                

                var query = Query.EQ("_id", trackerData._id);

                _mongoCollection.Update(query, update, UpdateFlags.Upsert);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.Write(ex.Message);
            }
        }
    }
}
