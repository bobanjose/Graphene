Graphene
========

A highly saleable BI framwork writing in C#. Graphene can be used to track various Metrics. Graphene pre-aggregates the data and uses a document store (like MongoDB/RavenDB) to store the pre-aggregated data. Graphene also supports adding filters to a Metrics.


## Getting Started

### Initializing Graphene

Add the line below to your applications initialization event (e.g. ASP.Net: Application_Start event, Windows Service: OnStart)

```c#
Graphene.Configurator.Initialize(
                    new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene") }
                );
```

### Creating a Tracker

A tracker is something of business value that needs to be tracked over time. Examples are number of order placed on a commerce site, the performance in MS of a given API etc. during a given time interval. 

To create a tracker implement Graphene.Tracking.ITrackable Interface

```c#
public class CustomerVisitTracker : ITrackable
{
	public string Name { get { return "Customer Visit Tracker"; } }

	public string Description { get { return "Counts the number of customer visits"; } }

	public Resolution MinResolution { get { return Resolution.Hour; } }

	public long KidsCount { get; set; }
    public long MiddleAgedCount { get; set; }
    public long ElderlyCount { get; set; }
}
```

The MinResolution set the minimum time interval that can be tracked. If the interval is set to Hour then one can generate reports of that metrics per hour and above (day/month etc.) but not below that.

The higher the resolution the better Graphene can pre-compute the data and better the queries will perform. 

### Incrementing a Tracker

#### Default Metrics

The line below increments the default tracker metric by 1

```c#
Tracking.Container<CustomerVisitTracker>.IncrementBy(1);
```
#### Named Metrics

The line below increments the named metric "MiddleAgedCount" by 1 and "ElderlyCount" by 2.

```c#
Tracking.Container<CustomerAgeTracker>
                        .Where<CustomerFilter>(
                            new CustomerFilter
                            {
                                State = "MN",
                                StoreID = "334",
                                Environment_ServerName = "Server2"
                            })
                        .Increment(e => e.MiddleAgedCount, 1)
                        .Increment(e => e.ElderlyCount, 2);
```

### Adding filters

Filters can be added to allow the tracker data to be sliced and diced. For example number of orders placed from a given state or API performance on a given server. 

First step to adding a filter is to define it as a struct.

```c#
public struct CustomerFilter
{
	public string State { get; set; }
	public string StoreID { get; set; }
	public string Gender { get; set; }
	public string Environment_ServerName { get; set; }
}
```

All properties must be string or a nullable type. For Graphene's pre-computation to be optimal the cardinality of the filter values should be low, i.e. the all combination of the filter values should be
on the lower side for best performance.

To apply a filter while incrementing a tracker follow the following sample. 

```c#
Graphene.Tracking.Container<CustomerVisitTracker>
	.Where<CustomerFilter>(
		new CustomerFilter
		{
			State = "CA",
			StoreID = "3234",
			Environment_ServerName = "Server1"
		}).IncrementBy(1);
```

### Shutting down Graphene

Add the line below to your applications shut down event (e.g. ASP.Net: Application_End event, Windows Service: OnStop)

```c#
Graphene.Configurator.ShutDown();
```

### Generating Reports

To generate reports initialize Graphene with ReportGenerator, currently Mongo DB is the only DB supported.

```c#
Graphene.Configurator.Initialize(
                      new Configuration.Settings() { Persister = new Publishing.PersistToMongo("mongodb://localhost/Graphene"), ReportGenerator = new Graphene.Mongo.Reporting.MongoReportGenerator("mongodb://localhost/Graphene") }
                  );
```

To generate reports for a given tracker without any filters:

```c#
var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Report(startTimeInUtc, endTimeInUtc);

Assert.IsTrue(report.Results.Count() >= 1);
Assert.IsTrue(report.Results[0].Tracker.ElderlyCount >= 12);
Assert.IsTrue(report.Results[0].Tracker.KidsCount >= 5);
```

Each item in "Results" is aggregated results for a resolution Graphene picked base on the total duration. The resolution picked is returned as "report.Resolution". The resolution can be year, month, day, hour or minute.

To specify a perticular resolution pass it in as a parameter to "Report". The resolution specified here should be at or higher then the resolution for the tracker else the tracker's resolution will take effect.

```c#
var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Report(startTimeInUtc, endTimeInUtc, ReportResolution.Minute);
```

The "Tracker" property is the same type as the Tracker its properties is the aggregated total for the duration specified in the resolution.

To apply a filter to the reports pass the filter criteria in the "Where" method.

```c#
var filter1 = new CustomerFilter
            {
                State = "CA"                
            };

var report = Graphene.Tracking.Container<TrackerWithCountProperties>.Where(filter1).Report(startTimeInUtc, endTimeInUtc);

```
 