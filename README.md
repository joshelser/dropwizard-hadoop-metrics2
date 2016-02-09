# Dropwizard Metrics Reporter to Hadoop Metrics2

This project is a simple Reporter which enables metrics collected by the
[Dropwizard Metrics](https://dropwizard.github.io/metrics) library to be
pushed to all configured [Hadoop Metrics2](https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/metrics2/package-summary.html)
system. This is done by creating a Reporter (Dropwizard) which is also a
MetricsSource (Hadoop Metrics2).

This combination allows applications instrumented via Dropwizard Metrics to send
their metrics to the [Ambari Metrics System](https://cwiki.apache.org/confluence/display/AMBARI/Metrics).

Most of the other functionality present in Hadoop Metrics2 is already
provided by Dropwizard Metrics. I personally prefer instrumenting code
with Dropwizard Metrics' API, so this is the perfect middle-ground.

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
