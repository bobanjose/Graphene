using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Graphene.Exceptions;
using Graphene.Tracking;

namespace Graphene.Reporting
{
    

    public class RestReportGenerator : IReportGenerator
    {
        private readonly Uri _serviceUri;


        public RestReportGenerator(Uri serviceUri)
        {
            _serviceUri = serviceUri;
        }


        public ITrackerReportResults GeneratorReport(IReportSpecification specification)
        {
            using (var httpClient = new HttpClient())
            {
                var task = httpClient.PostAsJsonAsync(_serviceUri.ToString(), specification)
                                         .ContinueWith(x => x.Result.Content.ReadAsAsync<IEnumerable<IAggregationResult>>().Result);

                task.ContinueWith(x =>
                {
                    if (task.IsFaulted)
                    {
                        if (task.Exception != null)
                            throw new ReportGenerationException(task.Exception);

                    }
                    IEnumerable<IAggregationResult> results = task.Result;
                    return results;
                });

                task.Wait();

            }
            return null;
        }

        public ITrackerReportResults GeneratorReport(IReportSpecification specification, bool differentiation)
        {
            using (var httpClient = new HttpClient())
            {
                var task = httpClient.PostAsJsonAsync(_serviceUri.ToString(), specification)
                                         .ContinueWith(x => x.Result.Content.ReadAsAsync<IEnumerable<IAggregationResult>>().Result);

                task.ContinueWith(x =>
                {
                    if (task.IsFaulted)
                    {
                        if (task.Exception != null)
                            throw new ReportGenerationException(task.Exception);

                    }
                    IEnumerable<IAggregationResult> results = task.Result;
                    return results;
                });

                task.Wait();

            }
            return null;
        }

    }


}

