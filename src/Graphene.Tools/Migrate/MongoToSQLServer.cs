using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Graphene.Data;
using Graphene.SQLServer;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Xml;


namespace Graphene.Tools.Migrate
{
    [DataContract(Namespace = "")]
    public class TrackerDataPoint
    {
        [DataMember]
        public string Id { get; set; }

        [DataMember]
        public string KeyFilter { get; set; }

        [DataMember]
        public Dictionary<string, long> Measurement { get; set; }

        [DataMember]
        public string Name { get; set; }

        [DataMember]
        public string[] SearchFilters { get; set; }

        [DataMember]
        public DateTime TimeSlot { get; set; }

        [DataMember]
        public String TypeName { get; set; }

        public string ToXml()
        {
            using (var sw = new StringWriter())
            {
                using (var xtw = new XmlTextWriter(sw))
                {
                    var serializer = new DataContractSerializer(typeof (TrackerDataPoint),"TrackerDataPoint","");
                    serializer.WriteObject(xtw, this);
                    return sw.ToString();
                }
            }
        }
    }


    public class MongoToSQLServer
    {
        private readonly string _sqlConnectionString;
        private static MongoCollection<TrackerDataPoint> _mongoCollection;
        private readonly StreamWriter logStream;
        private bool _stopCalled;
        private static readonly int DegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
        private static readonly DateTime StartTime = DateTime.Now;
        private static bool _deleteRecordAfterMigration;
        private DateTime _startDate;
        private DateTime _endDate;
        private int _daysInRange;

        public MongoToSQLServer(string sqlConnectionString, string mongoConnectionString, StreamWriter logger)
        {
            _sqlConnectionString = sqlConnectionString;
            string _mongoConnectionString = mongoConnectionString;

            MongoServer mongoServer = new MongoClient(_mongoConnectionString).GetServer();
            string databaseName = MongoUrl.Create(_mongoConnectionString).DatabaseName;
            MongoDatabase mongoDatabase = mongoServer.GetDatabase(databaseName);
            _mongoCollection = mongoDatabase.GetCollection<TrackerDataPoint>("TrackerData");
            logStream = logger;
        }

        public void Start(bool deleteRecordAfterMigration, DateTime startDate, DateTime endDate, int daysInRange = -1)
        {
            _deleteRecordAfterMigration = deleteRecordAfterMigration;

            _startDate =(startDate == DateTime.MinValue ? DateTime.Parse(ConfigurationManager.AppSettings["StartDate"]) : startDate).ToUniversalTime();
            _endDate = (endDate == DateTime.MaxValue ? DateTime.Parse(ConfigurationManager.AppSettings["EndDate"]) : endDate).ToUniversalTime();
            _daysInRange = daysInRange == -1 ? int.Parse(ConfigurationManager.AppSettings["TimeSpanInDays"]) : daysInRange;

            var dates = getDateRange(_startDate, _endDate, _daysInRange);
            var sqlIds = getSqlIds();
            Parallel.ForEach(dates, new ParallelOptions {MaxDegreeOfParallelism = DegreeOfParallelism},
                range => processDateRange(range.Item1, range.Item2, sqlIds.ToList()));

            var end = DateTime.Now;
            logStream.WriteLine("Job end: {0}", end.ToString("MM/dd/yy HH:mm:ss"));
            logStream.WriteLine("Document Transfer took {0} total", (end - StartTime).ToString("d'd 'hh'h 'mm'm 'ss's'"));
            logStream.WriteLine("Beginning data conversion {0}", DateTime.Now.ToString("MM/dd/yy HH:mm:ss"));
            startBulkConversion();
            logStream.WriteLine("Data conversion ended {0}", DateTime.Now.ToString("MM/dd/yy HH:mm:ss"));
            logStream.WriteLine("Data conversion took {0} total", (DateTime.Now - end).ToString("d'd 'hh'h 'mm'm 'ss's'"));
            logStream.WriteLine("Full Migration took {0} total", (DateTime.Now - StartTime).ToString("d'd 'hh'h 'mm'm 'ss's'"));
        }

        private void processDateRange(DateTime start, DateTime end, List<string> sqlIds)
        {
            var batchBegin = DateTime.Now;
            logStream.WriteLine("(+{0:N3}) Processing {1} to {2}", (batchBegin - StartTime).TotalSeconds, start, end);

            int rows;
            
            var constraints = new List<IMongoQuery>
            {
                Query.GTE("TimeSlot", start),
                Query.LT("TimeSlot", end),
            };

            var mongoRecords =
                _mongoCollection.Find(Query.And(constraints))
                    .ToList();

            var recordsToMove = mongoRecords
                .Where(tdp => !sqlIds.Contains(tdp.Id))
                .ToList();

            var table = CreateDataTable(recordsToMove);
            insertRecords(table);
            rows = table.Rows.Count;
            
            if (rows == 0)
            {
                logStream.WriteLine("(+{0:N3}) Processing {1} to {2}: nothing to do",
                    (DateTime.Now - StartTime).TotalSeconds, start.ToUniversalTime(), end.ToUniversalTime());
            }
            else
            {
                logStream.WriteLine("(+{0:N3}) Processing {1} to {2}: completed ({3} records in {4:N3}s)",
                    (DateTime.Now - StartTime).TotalSeconds, start.ToUniversalTime(), end.ToUniversalTime(), rows,
                    (DateTime.Now - batchBegin).TotalSeconds);
            }
            GC.Collect();
            GC.Collect();
            GC.Collect();
        }

        private static IEnumerable<Tuple<DateTime, DateTime>> getDateRange(DateTime startDate, DateTime endDate, int daysInRange)
        {
            return createRange(startDate, endDate, daysInRange);
        }

        private static IEnumerable<Tuple<DateTime, DateTime>> createRange(DateTime start, DateTime end, int days = 2)
        {
            var ret = new Tuple<DateTime, DateTime>(start, start.AddDays(days) > end ? end : start.AddDays(days));
            
            do
            {
                yield return ret;
                ret = new Tuple<DateTime, DateTime>(ret.Item2, ret.Item2.AddDays(days) > end ? end : ret.Item2.AddDays(days));
            } while (ret.Item2 < end);
            yield return ret;
        }

        
        public void Stop()
        {
            _stopCalled = true;
        }

        private void startBulkConversion()
        {
            var sqlConnStr = _sqlConnectionString;

            using (var connection = new SqlConnection(sqlConnStr))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "dbo.MigrateTrackers";
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandTimeout = 0;
                    command.ExecuteNonQuery();
                }
            }
        }

        private IEnumerable<string> getSqlIds()
        {
            var sqlConnStr = _sqlConnectionString;
            var sqlTableName = ConfigurationManager.AppSettings["SQLTableName"];

            List<string> idsList = new List<string>();
            using (var conn = new SqlConnection(sqlConnStr))
            {
                try
                {
                    conn.Open();
                }
                catch (SqlException ex)
                {
                    logStream.WriteLine("Unable to connect to Sql Server database.");
                    if (ex.Number != 10060)
                    {
                        throw;
                    }
                }
                SqlDataReader result;
                using (var cmd = new SqlCommand("SELECT Tracker_Document_Id FROM " + sqlTableName, conn))
                {
                    try
                    {
                        result = cmd.ExecuteReader();

                    }
                    catch (SqlException ex)
                    {
                        if (ex.Number == 208)
                        {
                            using (
                                var createTable =
                                    new SqlCommand(
                                        "CREATE TABLE [dbo].[TrackerDocuments]([Tracker_Document_Id] [nvarchar](400) NOT NULL,[Tracker_Json_Document] [nvarchar](MAX) NOT NULL,[Tracker_Xml_Document] [xml] NULL,[Document_Processed_By_Sproc] [nchar](1) NULL,[Document_Converted_By_Bulk_Load] [nchar](1) NULL) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]",
                                        conn))
                            {
                                try
                                {
                                    createTable.ExecuteNonQuery();
                                }
                                // ReSharper disable once EmptyGeneralCatchClause
                                catch
                                {
                                }
                            }
                            result = cmd.ExecuteReader();
                        }
                        else
                        {
                            throw;
                        }
                    }
                    catch (InvalidOperationException)
                    {
                        logStream.WriteLine("Cannot load document ids to skip.");
                        throw;
                    }
                    while (result.Read())
                    {
                        idsList.Add((string)result[0]);
                    }
                }
            }
            return idsList;
        }

        private static void insertRecords(DataTable values)
        {
            var sqlConnStr = ConfigurationManager.AppSettings["MSSQLConnectionString"];
            var sqlTableName = ConfigurationManager.AppSettings["SQLTableName"];

            using (var conn = new SqlConnection(sqlConnStr))
            {
                using (var adapter = new SqlDataAdapter("SELECT * FROM " + sqlTableName, conn))
                {
                    using (new SqlCommandBuilder(adapter))
                    {
                        try
                        {
                            adapter.FillLoadOption = LoadOption.Upsert;
                            adapter.Update(values);
                        }
                        catch (SqlException ex)
                        {
                            if (ex.Number != 2627)
                                throw;
                        }
                    }
                }
            }
        }

        public static DataTable CreateDataTable(IEnumerable<TrackerDataPoint> tdps)
        {
            var table = new DataTable();
            table.Columns.Add("Tracker_Document_Id", typeof (string));
            table.Columns.Add("Tracker_Json_Document", typeof (string));
            table.Columns.Add("Tracker_Xml_Document", typeof (string));
            table.Columns.Add("Document_Converted_By_Bulk_Load", typeof (char));

            foreach (var tdp in tdps)
            {
                table.Rows.Add(tdp.Id, tdp.ToJson(), tdp.ToXml(), 'N');
            }

            return table;
        }
    }
}

    