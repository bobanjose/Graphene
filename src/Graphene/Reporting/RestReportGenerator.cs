using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Graphene.Exceptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Graphene.Reporting
{
    public class RestReportGenerator : IReportGenerator
    {
        private readonly Uri _serviceUri;


        public RestReportGenerator(Uri serviceUri)
        {
            _serviceUri = serviceUri;
        }

        #region Implementation
        public ITrackerReportResults BuildReport(IReportSpecification specification)
        {
            using (var httpClient = new HttpClient())
            {
                Task<ITrackerReportResults> task = httpClient.PostAsJsonAsync(_serviceUri.ToString(),
                    specification).ContinueWith(x =>
                    {
                        if (x.IsFaulted)
                        {
                            if (x.Exception != null)
                                throw new ReportGenerationException(x.Exception);
                        }

                        var s = x.Result.Content.ReadAsStringAsync().Result;
                        var result = JsonConvert.DeserializeObject<ITrackerReportResults>(s,
                            new InternalTrackerReportResultsConverter(), new InternalAggregationResultConverter(),
                            new InternalMeasurementResultConverter(), new InternalMeasurementCollectionConverter(),
                            new InternalAggregationResultCollectionConverter());

                        return result;
                    });

                task.Wait();
                var trackerReportResult = task.Result;
                return trackerReportResult;
            }
        }

#endregion implementation


        #region Implementations
        internal class InternalAggregationResult : IAggregationResult
        {
            public ushort TimeSlice { get; set; }
            public string TypeName { get; set; }           
            public Dictionary<string, string> KeyFilters { get; set; }
            public DateTime? MesurementTimeUtc { get; set; }


            public IEnumerable<IMeasurementResult> MeasurementValues { get; set; }

            public long Occurence { get; set; }
            public long Total { get; set; }
          
        }

        internal class InternalTrackerReportResults : ITrackerReportResults
        {
            internal InternalTrackerReportResults()
            {
            }

            public DateTime FromDateUtc { get; set; }
            public DateTime ToDateUtc { get; set; }


            public IEnumerable<IAggregationResult> AggregationResults { get; set; }

            public ReportResolution resolution { get; set; }

            public IAggregationResult AddAggregationResult(DateTime mesurementTimeUtc, long occurence, long total)
            {
                throw new NotImplementedException();
            }
        }

        internal class InternalMeasurementResult : IMeasurementResult
        {
             
            public string PropertyName { get; set; }
            public string TrackerTypeName { get; set; }
            public string DisplayName { get; set; }
            public string Description { get; set; }
            public string FullyQualifiedPropertyName { get; set; }
            public long Value { get; set; }
        }
        #endregion Implementations

        #region converters
        internal class InternalTrackerReportResultsConverter : CustomCreationConverter<ITrackerReportResults>
        {
            /// <summary>
            /// Creates an object which will then be populated by the serializer.
            /// </summary>
            /// <param name="objectType">Type of the object.</param>
            /// <returns>
            /// The created object.
            /// </returns>
            public override ITrackerReportResults Create(Type objectType)
            {
                return new InternalTrackerReportResults();
            }
        }

        internal class InternalAggregationResultConverter : CustomCreationConverter<IAggregationResult>
        {
            /// <summary>
            /// Creates an object which will then be populated by the serializer.
            /// </summary>
            /// <param name="objectType">Type of the object.</param>
            /// <returns>
            /// The created object.
            /// </returns>
            public override IAggregationResult Create(Type objectType)
            {
                return new InternalAggregationResult();
            }
        }

        internal class InternalAggregationResultCollectionConverter :
            CustomCreationConverter<IEnumerable<IAggregationResult>>
        {
            /// <summary>
            /// Creates an object which will then be populated by the serializer.
            /// </summary>
            /// <param name="objectType">Type of the object.</param>
            /// <returns>
            /// The created object.
            /// </returns>
            public override IEnumerable<IAggregationResult> Create(Type objectType)
            {
                return new List<IAggregationResult>();
            }
        }



        internal class InternalMeasurementResultConverter : CustomCreationConverter<IMeasurementResult>
        {
            /// <summary>
            /// Creates an object which will then be populated by the serializer.
            /// </summary>
            /// <param name="objectType">Type of the object.</param>
            /// <returns>
            /// The created object.
            /// </returns>
            public override IMeasurementResult Create(Type objectType)
            {
                return new InternalMeasurementResult();
            }
        }

        internal class InternalMeasurementCollectionConverter : CustomCreationConverter<IEnumerable<IMeasurementResult>>
        {
            /// <summary>
            /// Creates an object which will then be populated by the serializer.
            /// </summary>
            /// <param name="objectType">Type of the object.</param>
            /// <returns>
            /// The created object.
            /// </returns>
            public override IEnumerable<IMeasurementResult> Create(Type objectType)
            {
                return new List<IMeasurementResult>();
            }
        }

        #endregion converters

    }
}
