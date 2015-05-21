using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Publishing;
using Graphene.SQLServer;
using Graphene.Tracking;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;

namespace Graphene.Tools.Migrate
{
   
    internal class ConsoleLogger : ILogger
    {
        public void Debug(string message)
        {
            Console.WriteLine(message);
        }

        public void Info(string message)
        {
            Console.WriteLine(message);
        }

        public void Warn(string message)
        {
            Console.WriteLine(message);
        }

        public void Error(string message, Exception ex)
        {
            Console.WriteLine(message);
        }
    }

    public class MigrateDataToV2
    {
        private readonly string _sqlConnectionString;
        private readonly string _mongoConnectionString;

        private MongoServer _mongoServer;
        private MongoDatabase _mongoDatabase;
        private ILogger _logger;

        private bool _stopCalled = false;

        public MigrateDataToV2(string sqlConnectionString, string mongoConnectionString, ILogger logger)
        {
            _sqlConnectionString = sqlConnectionString;
            _mongoConnectionString = mongoConnectionString;

            _mongoServer = new MongoClient(_mongoConnectionString).GetServer();
            string databaseName = MongoUrl.Create(_mongoConnectionString).DatabaseName;
            _mongoDatabase = _mongoServer.GetDatabase(databaseName);
            _logger = logger;
            Configurator.Initialize(
                new Settings
                {
                    Persister =
                        new PersistToSQLServer(@"Server=.\SQLServer2014;Database=GrapheneV18;Trusted_Connection=True;", logger),
                    ReportGenerator =
                        new SQLReportGenerator(@"Server=.\SQLServer2014;Database=GrapheneV18;Trusted_Connection=True;", logger),
                    Logger = logger
                });
        }

        public void Start(bool deleteRecordAfterMigration, int startAt, int stopAfter, int docsImported)
        {
            var docNumber = startAt;
            try
            {
                var trackerDocuments = _mongoDatabase.GetCollection("TrackerData");
                //var sqlPersister = new PersistToSQLServer(_sqlConnectionString, new ConsoleLogger());
                
                foreach (var trackerDocument in trackerDocuments.FindAll().Skip(startAt))
                {
                    Debug.Write(trackerDocument);
                    TrackerData trackerData = null;

                    ITrackable tracker = null;
                    string trackerTypeName = null;
                    DateTime? timeSlot = null;

                    foreach (var element in trackerDocument.Elements)
                    {
                        if (element.Name == "TypeName")
                        {
                           trackerTypeName = element.Value.ToString();
                        }
                    }
                    
                    if (trackerTypeName != null)
                    {
                        trackerData = new TrackerData(trackerTypeName);
                        foreach (var element in trackerDocument.Elements)
                        {
                            switch (element.Name)
                            {
                                case "Name":
                                    trackerData.Name = element.Value.ToString();
                                    break;
                                case "KeyFilter":
                                    trackerData.KeyFilter = element.Value.ToString();
                                    break;
                                case "TimeSlot":
                                    timeSlot = Convert.ToDateTime(element.Value.ToString());
                                    break;
                                //case "SearchFilters":
                                //    var filters =
                                //        element.Value.AsBsonArray.Select(p => p.AsString).ToArray();

                                //    for (var i = 0; i < filters.Length; i++)
                                //    {
                                //        filters[i] = filters[i].Split(new [] {",,"}, StringSplitOptions.None).OrderBy(f => f).Aggregate((x, z) => string.Concat(x, ",,", z));//this is to fix a bug that was caused by the missing sort in the previous version.
                                //    }
                                //    trakerData.SearchFilters = filters;
                                //    break;
                                case "Measurement":
                                    var mesurementDoc = (BsonDocument) element.Value;
                                    trackerData.Measurement = new Measure();
                                    trackerData.Measurement.NamedMetrics = new ConcurrentDictionary<string, long>();
                                    foreach (var measure in mesurementDoc.Elements)
                                    {
                                        //switch (measure.Name)
                                        //{
                                        //    case "_Total":
                                        //        trakerData.Measurement._Total = Convert.ToInt32(measure.Value.ToString());
                                        //        break;
                                        //    case "_Occurrence":
                                        //        trakerData.Measurement._Occurrence =
                                        //            Convert.ToInt32(measure.Value.ToString());
                                        //        break;
                                        //    case "_Min":
                                        //        trakerData.Measurement._Min = Convert.ToInt32(measure.Value.ToString());
                                        //        break;
                                        //    case "_Max":
                                        //        trakerData.Measurement._Max = Convert.ToInt32(measure.Value.ToString());
                                        //        break;
                                        //    default:
                                                var val = Convert.ToInt32(measure.Value);
                                                trackerData.Measurement.NamedMetrics.AddOrUpdate(measure.Name, val,
                                                    (i, t) => t + val);
                                        //        break;
                                        //}
                                    }
                                    break;
                            }
                        }

                        tracker = TrackerBuilder.CreateTrackerFromTypeName(trackerTypeName, trackerData.Name);

                        object filter = null;
                        if (!string.IsNullOrWhiteSpace(trackerData.KeyFilter))
                            filter = TrackerBuilder.CreateFilterObject(trackerData.KeyFilter);

                        var grapheneContainer = typeof(Container<>).MakeGenericType(tracker.GetType());
                        var grapheneContainerInstance = Activator.CreateInstance(grapheneContainer);
                        
                       // var grapheneSetTrackerType =
                      //      grapheneContainer.GetMethod("SetTrackerType", BindingFlags.Instance | BindingFlags.NonPublic);
                       // grapheneSetTrackerType.Invoke(grapheneContainerInstance, new[] { tracker });

                        var grapheneIncrement =
                            grapheneContainer.GetMethod("Increment", BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder,
                                    new[] { typeof(ITrackable), typeof(Resolution), typeof(DateTime), typeof(string), typeof(long), typeof(object) }, null);

                        foreach (var metric in trackerData.Measurement.NamedMetrics)
                        {
                            grapheneIncrement.Invoke(grapheneContainerInstance, new[] {tracker, Resolution.Day, timeSlot, metric.Key, metric.Value, filter });
                        }

                        if (deleteRecordAfterMigration)
                            trackerDocuments.Remove(Query<TrackerData>.EQ(td => td._id, trackerData._id));
                        if (_logger != null)
                            _logger.Info(string.Format("Migrated Document with ID:{0}, Number{1}", trackerData._id,
                                docNumber));
                        docNumber++;
                        docsImported++;
                        if (_stopCalled || (stopAfter!=-1 && docsImported >= stopAfter))
                            break;
                        
                    }
                }
            }
            catch (Exception exception)
            {
                _logger.Error(exception.Message, exception);
                if (exception is MongoDB.Driver.MongoQueryException && exception.Message == "Cursor not found.")//todo: find out the exception type. 
                {
                    Start(deleteRecordAfterMigration, docNumber, stopAfter, docsImported);
                }
            }
        }

        public void Stop()
        {
            _stopCalled = true;
        }
    }
}
    