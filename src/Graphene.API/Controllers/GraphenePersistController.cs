using System;
using System.Configuration;
using System.Web.Http;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Mongo.Publishing;
using Graphene.Publishing;
using Graphene.SQLServer;
using log4net;
using log4net.Repository.Hierarchy;

namespace Graphene.API.Controllers
{
    public class GraphenePersistController : ApiController
    {
        private static IPersist _mongoPersist;
        private static IPersist _sqlPersist;

        private static readonly ILog _logger =
           LogManager.GetLogger(typeof(GraphenePersistController));

        public GraphenePersistController()
        {
            if (!string.IsNullOrWhiteSpace(
                    ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString))
                _mongoPersist = new PersistToMongo(ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString,
                        new Log4NetLogger(_logger));

            if (!string.IsNullOrWhiteSpace(
                    ConfigurationManager.ConnectionStrings["SQLConnectionString"].ConnectionString))
                _sqlPersist = new PersistToSQLServer(ConfigurationManager.ConnectionStrings["SQLConnectionString"].ConnectionString,
                        new Log4NetLogger(_logger));
        }

        // POST api/values
        public void Post([FromBody] TrackerData trackerData)
        {
            try
            {
                if (_mongoPersist != null)
                    _mongoPersist.Persist(trackerData);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message, ex);
                throw;
            }

            try
            {
            if (_sqlPersist != null)
                _sqlPersist.Persist(trackerData);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.Message, ex);
                throw;
            }
        }
    }

    internal class Log4NetLogger : ILogger
    {
        private readonly ILog _logger;

        internal Log4NetLogger(ILog logger)
        {
            _logger = logger;
        } 

        public void Debug(string message)
        {
            _logger.Debug(message);
        }

        public void Info(string message)
        {
            _logger.Info(message);
        }

        public void Warn(string message)
        {
            _logger.Warn(message);
        }

        public void Error(string message, Exception ex)
        {
            _logger.Error(message);
        }
    }

    internal class SysDiagLogger : ILogger
    {
        public void Debug(string message)
        {
            System.Diagnostics.Debug.Write(message);
        }

        public void Info(string message)
        {
            System.Diagnostics.Debug.Write(message);
        }

        public void Warn(string message)
        {
            System.Diagnostics.Debug.Write(message);
        }

        public void Error(string message, Exception ex)
        {
            System.Diagnostics.Debug.Write(message);
        }
    }
}