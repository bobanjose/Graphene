using System;
using System.Net.Http;
using Graphene.Data;

namespace Graphene.Publishing
{
    public class PersistToService : IPersist
    {
        private readonly string _serviceUrl;

        public PersistToService(string serviceUrl)
        {
            _serviceUrl = serviceUrl;
        }

        public void Persist(TrackerData trackerData)
        {
            // setup the client
            // -   with a custome handler to approve all server certificates
            var handler = new WebRequestHandler
            {
                ServerCertificateValidationCallback =
                    (sender, certificate, chain, errors) => true
            };
            try
            {
                using (var client = new HttpClient(handler))
                {
                    // post it
                    HttpResponseMessage httpResponseMessage = client.PostAsJsonAsync(_serviceUrl, trackerData).Result;
                    if (!httpResponseMessage.IsSuccessStatusCode)
                    {
                        Configurator.Configuration.Logger.Warn(string.Format("Data not persisted with status {0}",
                            httpResponseMessage.StatusCode));
                    }
                }
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
            }
        }
    }
}