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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;
import com.github.joshelser.dropwizard.metrics.hadoop.MetricSnapshotFactory;

/**
 * Implementation of {@link MetricSnapshotFactory}.
 */
public class MetricSnapshotFactoryImpl implements MetricSnapshotFactory {

  private final HadoopMetrics2Reporter reporter;

  public MetricSnapshotFactoryImpl(HadoopMetrics2Reporter reporter) {
    this.reporter = reporter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Metric> MetricSnapshot<T> snapshot(T metric) {
    // We know that the parameterization is safe, but the compiler isn't smart enough to get that
    // in this form.
    if (metric instanceof Gauge) {
      return (MetricSnapshot<T>) snapshot((Gauge<?>) metric);
    } else if (metric instanceof Meter) {
      return (MetricSnapshot<T>) snapshot((Meter) metric);
    } else if (metric instanceof Counter) {
      return (MetricSnapshot<T>) snapshot((Counter) metric);
    } else if (metric instanceof Histogram) {
      return (MetricSnapshot<T>) snapshot((Histogram) metric);
    } else if (metric instanceof Timer) {
      return (MetricSnapshot<T>) snapshot((Timer) metric);
    } else {
      throw new IllegalArgumentException("Cannot handle metric of type: " + metric.getClass());
    }
  }

  @Override
  public GaugeSnapshot snapshot(Gauge<?> gauge) {
    return new GaugeSnapshot(gauge.getValue());
  }

  @Override
  public MeterSnapshot snapshot(Meter meter) {
    return new MeterSnapshot(reporter, meter);
  }

  @Override
  public CounterSnapshot snapshot(Counter counter) {
    return new CounterSnapshot(counter.getCount());
  }

  @Override
  public HistogramSnapshot snapshot(Histogram histogram) {
    return new HistogramSnapshot(reporter, histogram);
  }

  @Override
  public TimerSnapshot snapshot(Timer timer) {
    return new TimerSnapshot(reporter, timer.getCount(), timer.getMeanRate(), timer.getOneMinuteRate(),
        timer.getFiveMinuteRate(), timer.getFifteenMinuteRate(), timer.getSnapshot());
  }
}
