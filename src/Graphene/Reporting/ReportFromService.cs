using System;
using System.Net.Http;

namespace Graphene.Reporting
{
    public class ReportFromService : IReportGenerator
    {
        private readonly string _serviceUrl;

        public ReportFromService(string serviceUrl)
        {
            _serviceUrl = serviceUrl;
        }

        public ITrackerReportResults GeneratorReport(IReportSpecification specification)
        {
            //this has not been tested!!!!
            try
            {
                using (var client = new HttpClient())
                {
                    HttpResponseMessage httpResponseMessage = client.PostAsJsonAsync(_serviceUrl, specification).Result;
                    if (!httpResponseMessage.IsSuccessStatusCode)
                    {
                        Configurator.Configuration.Logger.Warn(string.Format("Getting report data failed {0}",
                            httpResponseMessage.StatusCode));
                    }
                    return httpResponseMessage.Content.ReadAsAsync<ITrackerReportResults>().Result;
                }
            }
            catch (Exception ex)
            {
                Configurator.Configuration.Logger.Error(ex.Message, ex);
                throw ex;
            }
        }
    }
}