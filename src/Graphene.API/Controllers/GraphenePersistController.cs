using System;
using System.Collections.Generic;
using System.Configuration;
using System.Web.Http;
using Graphene.Configuration;
using Graphene.Data;
using Graphene.Mongo.Publishing;
using Graphene.Publishing;
using Graphene.SQLServer;
using Graphene.Tracking;
using log4net;
using log4net.Repository.Hierarchy;
using Microsoft.Data.Edm;

namespace Graphene.API.Controllers
{
    public class GraphenePersistController : ApiController
    {
        private static IEnumerable<IPersist> _persisters;

        private readonly ILogger _logger;

        public GraphenePersistController(ILogger logger, IEnumerable<IPersist> persisters)
        {
            _logger = logger;
            _persisters = persisters;
        }

        // POST api/values
        public void Post([FromBody] TrackerData trackerData)
        {
            if (trackerData.Measurement == null)
                throw new Exception("TrackerData.Measurement is null - Form Body = " + Request.Content.ReadAsStringAsync().Result);
            if (trackerData.Measurement.CoveredResolutions == null)
            {
                trackerData.OverrideMinResolution(Resolution.NA);
                trackerData.Measurement.CoveredResolutions = new List<Resolution>();
                trackerData.TimeSlot = trackerData.TimeSlot.ToUniversalTime();
            }

            if (trackerData.TimeSlot.Kind == DateTimeKind.Unspecified)
            {
                trackerData.TimeSlot = DateTime.SpecifyKind(trackerData.TimeSlot,DateTimeKind.Utc) ;
            }

            foreach (var persister in _persisters)
            {
                try
                {
                    persister.Persist(trackerData);
                }
                catch (Exception ex)
                {
                    _logger.Error(ex.Message, ex);
                }
            }
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