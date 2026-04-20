using Graphene.Data;
using System;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;

namespace Graphene.Publishing
{
    public class PersistToService : IPersist
    {
        private readonly string _serviceUrl;

        public PersistToService(string serviceUrl)
        {
            _serviceUrl = serviceUrl;
        }

        public async Task PersistAsync(TrackerData trackerData)
        {
            // setup the client
            // -   with a custome handler to approve all server certificates
            var handler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback =
                (httpRequestMessage, certificate, chain, sslPolicyErrors) => true
            };

            var httpClient = new HttpClient(handler);

            try
            {
                using (var client = new HttpClient(handler))
                {
                    // post it
                    HttpResponseMessage httpResponseMessage = await client.PostAsJsonAsync(_serviceUrl, trackerData);

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
        

        public bool PersistPreAggregatedBuckets { get; set; }
    }
}