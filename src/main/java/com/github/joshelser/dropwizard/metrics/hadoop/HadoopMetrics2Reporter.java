/*
 * Copyright 2016 Josh Elser
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.dropwizard.metrics.hadoop;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A {@link com.codahale.metrics.Reporter} which also acts as a Hadoop Metrics2
 * {@link MetricsSource}. Configure it like other Reporters.
 *
 * <pre>
 * final HadoopMetrics2Reporter metrics2Reporter = HadoopMetrics2Reporter.forRegistry(metrics)
 *     .build(DefaultMetricsSystem.initialize("Phoenix"), // The application-level name
 *            "QueryServer", // Component name
 *            "Phoenix Query Server", // Component description
 *            "General"); // Name for each metric record
 *
 * metrics2Reporter.start(30, TimeUnit.SECONDS);
 * </pre>
 */
public class HadoopMetrics2Reporter extends ScheduledReporter implements MetricsSource {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMetrics2Reporter.class);
  private static final String EMPTY_STRING = "";

  public static final MetricsInfo RATE_UNIT_LABEL =
      Interns.info("rate_unit", "The unit of measure for rate metrics");
  public static final MetricsInfo DURATION_UNIT_LABEL =
      Interns.info("duration_unit", "The unit of measure of duration metrics");
  public static final int DEFAULT_MAXIMUM_METRICS_PER_TYPE = 1000;

  /**
   * Returns a new {@link Builder} for {@link HadoopMetrics2Reporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link HadoopMetrics2Reporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder to create {@link HadoopMetrics2Reporter} instances.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private MetricFilter filter;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private String recordContext;
    private int maxMetricsPerType = DEFAULT_MAXIMUM_METRICS_PER_TYPE;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.filter = MetricFilter.ALL;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
    }

    /**
     * Convert rates to the given time unit. Defaults to {@link TimeUnit#SECONDS}.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = Objects.requireNonNull(rateUnit);
      return this;
    }

    /**
     * Convert durations to the given time unit. Defaults to {@link TimeUnit#MILLISECONDS}.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = Objects.requireNonNull(durationUnit);
      return this;
    }

    /**
     * Only report metrics which match the given filter. Defaults to {@link MetricFilter#ALL}.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = Objects.requireNonNull(filter);
      return this;
    }

    /**
     * A "context" name that will be added as a tag on each emitted metric record. Defaults to
     * no "context" attribute on each record.
     *
     * @param recordContext The "context" tag
     * @return {@code this}
     */
    public Builder recordContext(String recordContext) {
      this.recordContext = Objects.requireNonNull(recordContext);
      return this;
    }

    /**
     * Overrides the default maximum number of Dropwizard {@link Metric} instances which are cached
     * to be passed to the Hadoop Metrics2 {@link MetricsCollector}. Default is {@link #DEFAULT_MAXIMUM_METRICS_PER_TYPE}.
     * Negative values are interpreted as {@link Integer#MAX_VALUE} and a value of zero is disallowed. 
     *
     * @param maxMetricsPerType The number of metric instances which are allowed by type.
     * @return {@code this}
     */
    public Builder maximumCachedMetricsPerType(int maxMetricsPerType) {
      if (0 < maxMetricsPerType) {
        this.maxMetricsPerType = maxMetricsPerType;
      } else if (0 > maxMetricsPerType) {
        this.maxMetricsPerType = Integer.MAX_VALUE;
      } else {
        throw new IllegalArgumentException("Maximum number of cached metrics of zero (0) is disallowed");
      }
      return this;
    }

    /**
     * Builds a {@link HadoopMetrics2Reporter} with the given properties, making metrics available
     * to the Hadoop Metrics2 framework (any configured {@link MetricsSource}s.
     *
     * @param metrics2System The Hadoop Metrics2 system instance.
     * @param jmxContext The JMX "path", e.g. {@code "MyServer,sub=Requests"}.
     * @param description A description these metrics.
     * @param recordName A suffix included on each record to identify it.
     *
     * @return a {@link HadoopMetrics2Reporter}
     */
    public HadoopMetrics2Reporter build(MetricsSystem metrics2System, String jmxContext,
        String description, String recordName) {
      return new HadoopMetrics2Reporter(registry,
          rateUnit,
          durationUnit,
          filter,
          metrics2System,
          Objects.requireNonNull(jmxContext),
          description,
          recordName,
          recordContext,
          this.maxMetricsPerType);
    }
  }

  private final MetricsRegistry metrics2Registry;
  private final MetricsSystem metrics2System;
  private final String recordName;
  private final String context;
  private final int maxMetricsPerType;

  // TODO Adding to the queues and removing from them are now guarded by explicit synchronization
  // so these don't need to be safe for concurrency anymore.
  @SuppressWarnings("rawtypes")
  private final ArrayBlockingQueue<Entry<String, Gauge>> dropwizardGauges;
  private final ArrayBlockingQueue<Entry<String, Counter>> dropwizardCounters;
  private final ArrayBlockingQueue<Entry<String, Histogram>> dropwizardHistograms;
  private final ArrayBlockingQueue<Entry<String, Meter>> dropwizardMeters;
  private final ArrayBlockingQueue<Entry<String, Timer>> dropwizardTimers;

  private HadoopMetrics2Reporter(MetricRegistry registry, TimeUnit rateUnit, TimeUnit durationUnit,
      MetricFilter filter, MetricsSystem metrics2System, String jmxContext, String description,
      String recordName, String context, int maxMetricsPerType) {
    super(registry, "hadoop-metrics2-reporter", filter, rateUnit, durationUnit);
    this.metrics2Registry = new MetricsRegistry(Interns.info(jmxContext, description));
    this.metrics2System = metrics2System;
    this.recordName = recordName;
    this.context = context;
    this.maxMetricsPerType = maxMetricsPerType;

    this.dropwizardGauges = new ArrayBlockingQueue<>(maxMetricsPerType);
    this.dropwizardCounters = new ArrayBlockingQueue<>(maxMetricsPerType);
    this.dropwizardHistograms = new ArrayBlockingQueue<>(maxMetricsPerType);
    this.dropwizardMeters = new ArrayBlockingQueue<>(maxMetricsPerType);
    this.dropwizardTimers = new ArrayBlockingQueue<>(maxMetricsPerType);

    // Register this source with the Metrics2 system.
    // Make sure this is the last thing done as getMetrics() can be called at any time after.
    this.metrics2System.register(Objects.requireNonNull(jmxContext),
        Objects.requireNonNull(description), this);
  }

  @Override public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(recordName);
    if (null != context) {
      builder.setContext(context);
    }

    // Synchronizing here ensures that the dropwizard metrics collection side is excluded from executing
    // at the same time we are pulling elements from the queues.
    synchronized (this) {
      snapshotAllMetrics(builder);
    }

    metrics2Registry.snapshot(builder, all);
  }

  /**
   * Consumes the current metrics collected by dropwizard and adds them to the {@code builder}.
   *
   * @param builder A record builder
   */
  void snapshotAllMetrics(MetricsRecordBuilder builder) {
    // Pass through the gauges
    @SuppressWarnings("rawtypes")
    Iterator<Entry<String, Gauge>> gaugeIterator = dropwizardGauges.iterator();
    while (gaugeIterator.hasNext()) {
      @SuppressWarnings("rawtypes")
      Entry<String, Gauge> gauge = gaugeIterator.next();
      final MetricsInfo info = Interns.info(gauge.getKey(), EMPTY_STRING);
      final Object o = gauge.getValue().getValue();

      // Figure out which gauge types metrics2 supports and call the right method
      if (o instanceof Integer) {
        builder.addGauge(info, (int) o);
      } else if (o instanceof Long) {
        builder.addGauge(info, (long) o);
      } else if (o instanceof Float) {
        builder.addGauge(info, (float) o);
      } else if (o instanceof Double) {
        builder.addGauge(info, (double) o);
      } else {
        LOG.debug("Ignoring Gauge ({}) with unhandled type: {}", gauge.getKey(), o.getClass());
      }

      gaugeIterator.remove();
    }

    // Pass through the counters
    Iterator<Entry<String, Counter>> counterIterator = dropwizardCounters.iterator();
    while (counterIterator.hasNext()) {
      Entry<String, Counter> counter = counterIterator.next();
      MetricsInfo info = Interns.info(counter.getKey(), EMPTY_STRING);
      LOG.debug("Adding counter {} {}", info, counter.getValue().getCount());
      builder.addCounter(info, counter.getValue().getCount());
      counterIterator.remove();
    }

    // Pass through the histograms
    Iterator<Entry<String, Histogram>> histogramIterator = dropwizardHistograms.iterator();
    while (histogramIterator.hasNext()) {
      final Entry<String, Histogram> entry = histogramIterator.next();
      final String name = entry.getKey();
      final Histogram histogram = entry.getValue();

      addSnapshot(builder, name, EMPTY_STRING, histogram.getSnapshot(), histogram.getCount());

      histogramIterator.remove();
    }

    // Pass through the meter values
    Iterator<Entry<String, Meter>> meterIterator = dropwizardMeters.iterator();
    while (meterIterator.hasNext()) {
      final Entry<String, Meter> meterEntry = meterIterator.next();
      final String name = meterEntry.getKey();
      final Meter meter = meterEntry.getValue();

      addMeter(builder, name, EMPTY_STRING, meter.getCount(), meter.getMeanRate(),
          meter.getOneMinuteRate(), meter.getFiveMinuteRate(), meter.getFifteenMinuteRate());

      meterIterator.remove();
    }

    // Pass through the timers (meter + histogram)
    Iterator<Entry<String, Timer>> timerIterator = dropwizardTimers.iterator();
    while (timerIterator.hasNext()) {
      final Entry<String, Timer> timerEntry = timerIterator.next();
      final String name = timerEntry.getKey();
      final Timer timer = timerEntry.getValue();
      final Snapshot snapshot = timer.getSnapshot();

      // Add the meter info (mean rate and rate over time windows)
      addMeter(builder, name, EMPTY_STRING, timer.getCount(), timer.getMeanRate(),
          timer.getOneMinuteRate(), timer.getFiveMinuteRate(), timer.getFifteenMinuteRate());

      // Count was already added via the meter
      addSnapshot(builder, name, EMPTY_STRING, snapshot);

      timerIterator.remove();
    }

    // Add in metadata about what the units the reported metrics are displayed using.
    builder.tag(RATE_UNIT_LABEL, getRateUnit());
    builder.tag(DURATION_UNIT_LABEL, getDurationUnit());
  }

  /**
   * Add Dropwizard-Metrics rate information to a Hadoop-Metrics2 record builder, converting the
   * rates to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   * @param desc A description for the record.
   * @param count The number of measured events.
   * @param meanRate The average measured rate.
   * @param oneMinuteRate The measured rate over the past minute.
   * @param fiveMinuteRate The measured rate over the past five minutes
   * @param fifteenMinuteRate The measured rate over the past fifteen minutes.
   */
  private void addMeter(MetricsRecordBuilder builder, String name, String desc, long count,
      double meanRate, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate) {
    builder.addGauge(Interns.info(name + "_count", EMPTY_STRING), count);
    builder.addGauge(Interns.info(name + "_mean_rate", EMPTY_STRING), convertRate(meanRate));
    builder.addGauge(Interns.info(name + "_1min_rate", EMPTY_STRING), convertRate(oneMinuteRate));
    builder.addGauge(Interns.info(name + "_5min_rate", EMPTY_STRING), convertRate(fiveMinuteRate));
    builder.addGauge(Interns.info(name + "_15min_rate", EMPTY_STRING),
        convertRate(fifteenMinuteRate));
  }

  /**
   * Add Dropwizard-Metrics value-distribution data to a Hadoop-Metrics2 record building, converting
   * the durations to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   * @param desc A description for this record.
   * @param snapshot The distribution of measured values.
   * @param count The number of values which were measured.
   */
  private void addSnapshot(MetricsRecordBuilder builder, String name, String desc,
      Snapshot snapshot, long count) {
    builder.addGauge(Interns.info(name + "_count", desc), count);
    addSnapshot(builder, name, desc, snapshot);
  }

  /**
   * Add Dropwizard-Metrics value-distribution data to a Hadoop-Metrics2 record building, converting
   * the durations to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   * @param desc A description for this record.
   * @param snapshot The distribution of measured values.
   */
  private void addSnapshot(MetricsRecordBuilder builder, String name, String desc,
      Snapshot snapshot) {
    builder.addGauge(Interns.info(name + "_mean", desc), convertDuration(snapshot.getMean()));
    builder.addGauge(Interns.info(name + "_min", desc), convertDuration(snapshot.getMin()));
    builder.addGauge(Interns.info(name + "_max", desc), convertDuration(snapshot.getMax()));
    builder.addGauge(Interns.info(name + "_median", desc), convertDuration(snapshot.getMedian()));
    builder.addGauge(Interns.info(name + "_stddev", desc), convertDuration(snapshot.getStdDev()));

    builder.addGauge(Interns.info(name + "_75thpercentile", desc),
        convertDuration(snapshot.get75thPercentile()));
    builder.addGauge(Interns.info(name + "_95thpercentile", desc),
        convertDuration(snapshot.get95thPercentile()));
    builder.addGauge(Interns.info(name + "_98thpercentile", desc),
        convertDuration(snapshot.get98thPercentile()));
    builder.addGauge(Interns.info(name + "_99thpercentile", desc),
        convertDuration(snapshot.get99thPercentile()));
    builder.addGauge(Interns.info(name + "_999thpercentile", desc),
        convertDuration(snapshot.get999thPercentile()));
  }

  @SuppressWarnings("rawtypes")
  @Override public void report(SortedMap<String, Gauge> gauges, 
      SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    // ScheduledReporter is synchronizing on `this`, so we don't have to worry about concurrent
    // invocations of reporter causing trouble.
    addEntriesToQueue(dropwizardGauges, gauges);
    addEntriesToQueue(dropwizardCounters, counters);
    addEntriesToQueue(dropwizardHistograms, histograms);
    addEntriesToQueue(dropwizardMeters, meters);
    addEntriesToQueue(dropwizardTimers, timers);
  }

  /**
   * Attempts to add all of the elements provided in {@code metrics} to the given {@code queue}. Upon
   * the first failure to add an element, the head of the queue will be drained by the number of
   * metrics still remaining to add, and the element will be re-offered. Upon subsequent failure to
   * add the same element, this method will exit and not add the current or any future elements to the queue.
   *
   * @param queue The queue to add elements to
   * @param metrics The metrics elements to add to the queue
   */
  protected <T> void addEntriesToQueue(ArrayBlockingQueue<Entry<String,T>> queue, SortedMap<String,T> metrics) {
    final Iterator<Entry<String,T>> metricsToAdd = metrics.entrySet().iterator();
    int entriesLeftToAdd = metrics.size();

    // The number of metrics to add exceeds the number of metrics we can store, clear out the queue
    // while grabbing the lock once to be slightly more efficient.
    if (entriesLeftToAdd >= getMaxMetricsPerType()) {
      queue.clear();

      // If we have more metrics to add than we can cache, trim entries off the beginning of the map
      // until we should be able to add all of the entries
      final int excessEntries = entriesLeftToAdd - getMaxMetricsPerType();
      // The SortedMap is also unmodifiable. Need to just read the entries, cannot remove them
      final int entriesRemoved = consumeIncomingMetrics(metricsToAdd, excessEntries);
      LOG.debug("Ignored {} incoming metric entries as they would not fit in the cache", entriesRemoved);
    }

    // Iterate over each metric we have to add
    while (metricsToAdd.hasNext()) {
      Entry<String,T> entry = metricsToAdd.next();
      // Assume that we have space (normal condition)
      if (!queue.offer(entry)) {
        LOG.debug("Failed to add metrics element. Removing {} elements from queue", entriesLeftToAdd);
        // If we fail to add the entry to the tail of the queue, try to free up enough space
        // for all remaining entries
        for (int i = 0; i < entriesLeftToAdd; i++) {
          if (null == queue.poll()) {
            // Queue is now empty
            break;
          }
        }
        // Re-attempt to add the metric
        if (!queue.offer(entry)) {
          // Failed a second time to add an element again after trying to free space. Give up and drop the remaining metrics
          LOG.debug("Failed to aggregate {} remaining metrics out of {}", entriesLeftToAdd, metrics.size());
          return;
        }
      }

      // We successfully added the current entry (either immediately, or after the poll()'s and re-offer())
      entriesLeftToAdd--;
    }
  }

  /**
   * Consumes (iterates over) {@code numEntriesToPrune} from {@code metrics}.
   *
   * @param metrics The metric entries to consume
   * @param numEntriesToConsume The number of entries to consume
   * @return the number of entries actually consumed
   */
  protected <T> int consumeIncomingMetrics(Iterator<Entry<String,T>> metrics, int numEntriesToConsume) {
    // noop
    if (numEntriesToConsume <= 0) {
      return 0;
    }

    int entriesConsumed = 0;

    // Keep removing metric items from the collection we have to cache until we can be sure they
    // will all fit in the cache.
    while (numEntriesToConsume > 0 && metrics.hasNext()) {
      metrics.next();
      numEntriesToConsume--;
      entriesConsumed++;
    }

    return entriesConsumed;
  }

  @Override protected String getRateUnit() {
    // Make it "events per rate_unit" to be accurate.
    return "events/" + super.getRateUnit();
  }

  @Override protected String getDurationUnit() {
    // Make it visible to the tests
    return super.getDurationUnit();
  }

  @Override protected double convertDuration(double duration) {
    // Make it visible to the tests
    return super.convertDuration(duration);
  }

  @Override protected double convertRate(double rate) {
    // Make it visible to the tests
    return super.convertRate(rate);
  }

  // Getters visible for testing

  MetricsRegistry getMetrics2Registry() {
    return metrics2Registry;
  }

  MetricsSystem getMetrics2System() {
    return metrics2System;
  }

  String getRecordName() {
    return recordName;
  }

  String getContext() {
    return context;
  }

  @SuppressWarnings("rawtypes") ArrayBlockingQueue<Entry<String, Gauge>> getDropwizardGauges() {
    return dropwizardGauges;
  }

  ArrayBlockingQueue<Entry<String, Counter>> getDropwizardCounters() {
    return dropwizardCounters;
  }

  ArrayBlockingQueue<Entry<String, Histogram>> getDropwizardHistograms() {
    return dropwizardHistograms;
  }

  ArrayBlockingQueue<Entry<String, Meter>> getDropwizardMeters() {
    return dropwizardMeters;
  }

  ArrayBlockingQueue<Entry<String, Timer>> getDropwizardTimers() {
    return dropwizardTimers;
  }

  protected void printQueueDebugMessage() {
    StringBuilder sb = new StringBuilder(64);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss");
    sb.append(sdf.format(new Date())).append(" ================================\n");
    sb.append("\n  Dropwizard gauges queue size: ").append(getDropwizardGauges().size());
    sb.append("\n  Dropwizard counters queue size: ").append(getDropwizardCounters().size());
    sb.append("\n  Dropwizard histograms queue size: ").append(getDropwizardHistograms().size());
    sb.append("\n  Dropwizard meters queue size: ").append(getDropwizardMeters().size());
    sb.append("\n  Dropwizard timers queue size: ").append(getDropwizardTimers().size()).append("\n");
    System.out.println(sb.toString());
  }

  int getMaxMetricsPerType() {
    return this.maxMetricsPerType;
  }
}
