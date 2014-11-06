using System;
using System.Configuration;
using System.Web.Http;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Mongo.Publishing;
using Graphene.Publishing;

namespace Graphene.API.Controllers
{
    public class GraphenePersistController : ApiController
    {
        private static readonly IPersist MongoPersist =
            new PersistToMongo(ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString, new SysDiagLogger());

        // POST api/values
        public void Post([FromBody] TrackerData trackerData)
        {
            MongoPersist.Persist(trackerData);
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