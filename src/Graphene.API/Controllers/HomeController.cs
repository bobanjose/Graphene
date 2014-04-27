using System.Reflection;
using System.Web.Mvc;

namespace Graphene.API.Controllers
{
    public class HomeController : Controller
    {
        //
        // GET: /Home/

        public string Index()
        {
            return Assembly.GetExecutingAssembly().FullName;
        }
    }
}