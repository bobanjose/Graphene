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
}
```

The MinResolution set the minimum time interval that can be tracked. If the interval is set to Hour then one can generate reports of that metrics per hour and above (day/month etc.) but not below that.

The higher the resolution the better Graphene can pre-compute the data and better the queries will perform. 

### Incrementing a Tracker

The line below increments the tracker by 1

```c#
Tracking.Container<CustomerVisitTracker>.IncrementBy(1);
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

This functionality is yet built. Generating reports would requiring querying the underlying Graphene data store directly.

Below is how the generated data looks like when using MongoDB as the data store.

```json
{
  "KeyFilter" : "ENVIRONMENT_SERVERNAME::SERVER2,STATE::MN,STOREID::334",
  "Measurement" : {
    "Occurrence" : NumberLong(23),
    "Total" : NumberLong(23)
  },
  "Name" : "Customer Visit Tracker",
  "SearchFilters" : ["STATE::MN,,STOREID::334", "ENVIRONMENT_SERVERNAME::SERVER2,,STOREID::334", "ENVIRONMENT_SERVERNAME::SERVER2,,STATE::MN", "STOREID::334", "ENVIRONMENT_SERVERNAME::SERVER2", "STATE::MN"],
  "TimeSlot" : ISODate("2014-01-13T02:00:00Z"),
  "_id" : "CustomerPurchaseTracker1/12/2014 6:00:00 PMENVIRONMENT_SERVERNAME::SERVER2,STATE::MN,STOREID::334"
}
```

So the above can be read as: The "Customer Visit Tracker" was incremented Measurement.Occurrence times and the total sum Measurement.Total for the TimeSlot of 2014-01-13, 02:00. There will be a document for every TimeSlot and if the MinResolution is 1 hour the next TimeSlot would be 2014-01-13, 04:00. Also there will be a document for every unique combination of the filters. Measurement.SearchFilters are the tags to for every possible combination of the filters delimited by ",,". To find all customer visits for the state of MN and store ID of 334 add the following to the query

```json
{ SearchFilters: { $in: ['STATE::MN,,STOREID::334' ] } } 
```

