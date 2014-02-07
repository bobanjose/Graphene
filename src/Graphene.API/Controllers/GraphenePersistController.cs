using System.Configuration;
using System.Web.Http;
using Graphene.Data;
using Graphene.Publishing;

namespace Graphene.API.Controllers
{
    public class GraphenePersistController : ApiController
    {
        private static readonly IPersist MongoPersist =
            new PersistToMongo(ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString);

        // POST api/values
        public void Post([FromBody] TrackerData trackerData)
        {
            MongoPersist.Persist(trackerData);
        }
    }
}