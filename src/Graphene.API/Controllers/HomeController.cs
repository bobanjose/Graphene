using System.Reflection;
using Microsoft.AspNetCore.Mvc;

namespace Graphene.API.Controllers
{
    public class HomeController : Controller
    {
        [HttpGet]
        public ActionResult<string> Index()
        {
            return Assembly.GetExecutingAssembly().FullName;
        }
    }
}