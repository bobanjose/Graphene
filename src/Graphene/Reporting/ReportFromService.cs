using System;
using System.Collections.Generic;
using System.Net.Http;
using Graphene.Data;

namespace Graphene.Reporting
{
    public class ReportFromService: IReportGenerator
    {
        private string _serviceUrl;

        public ReportFromService(string serviceUrl)
        {
            _serviceUrl = serviceUrl;
        }

        public IEnumerable<IQueryResults> GeneratorReport(IReportSpecification specification)
        {
            //this has not been tested!!!!
            try
            {
                using (var client = new HttpClient())
                {
                    var httpResponseMessage = client.PostAsJsonAsync(_serviceUrl, specification).Result;
                    if (!httpResponseMessage.IsSuccessStatusCode)
                    {
                        Configurator.Configuration.Logger.Warn(string.Format("Getting report data failed {0}", httpResponseMessage.StatusCode));
                    }
                    return httpResponseMessage.Content.ReadAsAsync<IEnumerable<IQueryResults>>().Result;
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
