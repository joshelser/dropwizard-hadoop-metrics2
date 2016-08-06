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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.CounterSnapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.GaugeSnapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.HistogramSnapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.MeterSnapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.MetricSnapshot;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.TimerSnapshot;

/**
 * Factory to create {@link MetricSnapshot}'s from a {@link Metric}.
 */
public interface MetricSnapshotFactory {

  <T extends Metric> MetricSnapshot<T> snapshot(T metric);

  GaugeSnapshot snapshot(Gauge<?> gauge);

  MeterSnapshot snapshot(Meter meter);

  CounterSnapshot snapshot(Counter counter);

  HistogramSnapshot snapshot(Histogram histogram);

  TimerSnapshot snapshot(Timer timer);

}
