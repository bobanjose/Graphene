﻿// Copyright 2013-2014 Boban Jose

using System;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Publishing;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Graphene.Mongo.Publishing
{
    public class PersistToMongo : IPersist
    {
        private const string COLLECTION_NAME = "TrackerData";
        private static MongoServer _mongoServer;
        private static MongoDatabase _mongoDatabase;
        private static MongoCollection _mongoCollection;
        private readonly ILogger _logger;

        public PersistToMongo(string connectionString, ILogger logger)
        {
            _mongoServer = new MongoClient(connectionString).GetServer();
            string databaseName = MongoUrl.Create(connectionString).DatabaseName;
            _mongoDatabase = _mongoServer.GetDatabase(databaseName);
            _mongoCollection = _mongoDatabase.GetCollection(COLLECTION_NAME);
            _logger = logger;
            PersistPreAggregatedBuckets = false;
        }

        public void Persist(TrackerData trackerData)
        {
            try
            {
                UpdateBuilder update2 = Update.Inc("Measurement._Total", trackerData.Measurement._Total)
                    .Inc("Measurement._Occurrence", trackerData.Measurement._Occurrence)
                    .SetOnInsert("TypeName", trackerData.TypeName)
                    .SetOnInsert("KeyFilter", trackerData.KeyFilter)
                    .SetOnInsert("Name", trackerData.Name)
                    .SetOnInsert("SearchFilters", new BsonArray(trackerData.SearchFilters))
                    .SetOnInsert("TimeSlot", trackerData.TimeSlot);

                foreach (var nm in trackerData.Measurement.NamedMetrics)
                {
                    update2.Inc("Measurement." + nm.Key, nm.Value);
                }

                IMongoQuery query = Query.EQ("_id", trackerData._id);

                _mongoCollection.Update(query, update2, UpdateFlags.Upsert);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message, ex);
            }
        }

        public bool PersistPreAggregatedBuckets { get; private set; }
    }
}