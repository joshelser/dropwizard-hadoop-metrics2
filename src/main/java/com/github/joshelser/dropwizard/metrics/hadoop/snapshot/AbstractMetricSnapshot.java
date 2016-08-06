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
package com.github.joshelser.dropwizard.metrics.hadoop.snapshot;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;

/**
 * Abstract class to hold common functionality across {@link MetricSnapshot} implementations. 
 */
public abstract class AbstractMetricSnapshot<T extends Metric> implements MetricSnapshot<T> {

  protected static final String EMPTY_STRING = "";

  private final HadoopMetrics2Reporter reporter;

  AbstractMetricSnapshot(HadoopMetrics2Reporter reporter) {
    this.reporter = reporter;
  }
  /**
   * Add Dropwizard-Metrics rate information to a Hadoop-Metrics2 record builder, converting the
   * rates to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param baseName A base name for this record.
   * @param meanRate The average measured rate.
   * @param oneMinuteRate The measured rate over the past minute.
   * @param fiveMinuteRate The measured rate over the past five minutes
   * @param fifteenMinuteRate The measured rate over the past fifteen minutes.
   */
  void snapshotMeter(MetricsRecordBuilder builder, String baseName,
      double meanRate, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate) {
    builder.addGauge(Interns.info(baseName + "_mean_rate", EMPTY_STRING), convertRate(meanRate));
    builder.addGauge(Interns.info(baseName + "_1min_rate", EMPTY_STRING), convertRate(oneMinuteRate));
    builder.addGauge(Interns.info(baseName + "_5min_rate", EMPTY_STRING), convertRate(fiveMinuteRate));
    builder.addGauge(Interns.info(baseName + "_15min_rate", EMPTY_STRING),
        convertRate(fifteenMinuteRate));
  }

  /**
   * Add Dropwizard-Metrics value-distribution data to a Hadoop-Metrics2 record building, converting
   * the durations to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param baseName A base name for this record.
   * @param snapshot The distribution of measured values.
   */
  void snapshotSnapshot(MetricsRecordBuilder builder, String baseName, Snapshot snapshot) {
    builder.addGauge(Interns.info(baseName + "_mean", EMPTY_STRING), convertDuration(snapshot.getMean()));
    builder.addGauge(Interns.info(baseName + "_min", EMPTY_STRING), convertDuration(snapshot.getMin()));
    builder.addGauge(Interns.info(baseName + "_max", EMPTY_STRING), convertDuration(snapshot.getMax()));
    builder.addGauge(Interns.info(baseName + "_median", EMPTY_STRING), convertDuration(snapshot.getMedian()));
    builder.addGauge(Interns.info(baseName + "_stddev", EMPTY_STRING), convertDuration(snapshot.getStdDev()));

    builder.addGauge(Interns.info(baseName + "_75thpercentile", EMPTY_STRING),
        convertDuration(snapshot.get75thPercentile()));
    builder.addGauge(Interns.info(baseName + "_95thpercentile", EMPTY_STRING),
        convertDuration(snapshot.get95thPercentile()));
    builder.addGauge(Interns.info(baseName + "_98thpercentile", EMPTY_STRING),
        convertDuration(snapshot.get98thPercentile()));
    builder.addGauge(Interns.info(baseName + "_99thpercentile", EMPTY_STRING),
        convertDuration(snapshot.get99thPercentile()));
    builder.addGauge(Interns.info(baseName + "_999thpercentile", EMPTY_STRING),
        convertDuration(snapshot.get999thPercentile()));
  }
  
  private double convertDuration(double value) {
    return reporter.convertDuration(value);
  }
  
  double convertRate(double value) {
    return reporter.convertRate(value);
  }
}
