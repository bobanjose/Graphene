using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Mongo.Publishing;
using Graphene.Publishing;
using Graphene.SQLServer;
using Graphene.Tracking;

namespace Graphene.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class GraphenePersistController : ControllerBase
    {
        private static IEnumerable<IPersist> _persisters;

        private readonly Graphene.Configuration.ILogger _logger;

        public GraphenePersistController(Graphene.Configuration.ILogger logger, IEnumerable<IPersist> persisters)
        {
            _logger = logger;
            _persisters = persisters;
        }

        [HttpPost]
        public IActionResult Post([FromBody] TrackerData trackerData)
        {
            if (trackerData.Measurement == null)
                throw new Exception("TrackerData.Measurement is null - Form Body could not be read");
            if (trackerData.Measurement.CoveredResolutions == null)
            {
                trackerData.OverrideMinResolution(Resolution.NA);
                trackerData.Measurement.CoveredResolutions = new List<Resolution>();
            }
            else
            {
                if (trackerData.TimeSlot.Kind == DateTimeKind.Unspecified)
                {
                    trackerData.TimeSlot = DateTime.SpecifyKind(trackerData.TimeSlot, DateTimeKind.Utc);
                }
            }

            foreach (var persister in _persisters)
            {
                try
                {
                    persister.PersistAsync(trackerData);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex.Message, ex);
                }
            }

            return Ok();
        }
    }
}