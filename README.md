# Dropwizard Metrics Reporter to Hadoop Metrics2

This project is a simple Reporter which enables metrics collected by the
[Dropwizard Metrics](https://dropwizard.github.io/metrics) library to be
pushed to all configured [Hadoop Metrics2](https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/metrics2/package-summary.html)
system. In my opinion, Dropwizard Metrics is the superior API that feels more
natural to use.

However, the intended means to push metrics into the [Ambari Metrics System](https://cwiki.apache.org/confluence/display/AMBARI/Metrics)
is provided as a Hadoop Metrics2 Sink. This library acts as a bridge for applications
to be instrumented via Dropwizard Metrics but to send those metrics to the Ambari
Metrics System.

## Usage

The Reporter is published in Maven Central and available for use:

```
<dependency>
  <groupId>com.github.joshelser</groupId>
  <artifactId>dropwizard-metrics-hadoop-metrics2-reporter</artifactId>
  <version>0.1.0</version>
</dependency>
```

It can be configured in the same fashion as the other available reporters:

```
final MetricRegistry metrics = new MetricRegistry();

final HadoopMetrics2Reporter metrics2Reporter = HadoopMetrics2Reporter.forRegistry(metrics)
    .build(DefaultMetricsSystem.initialize("Phoenix"), // The application-level name
           "QueryServer", // Component name
           "Phoenix Query Server", // Component description
           "General"); // Name for each metric record

metrics2Reporter.start(30, TimeUnit.SECONDS);
```

This example will push metrics to Hadoop Metrics2 every 30 seconds.
