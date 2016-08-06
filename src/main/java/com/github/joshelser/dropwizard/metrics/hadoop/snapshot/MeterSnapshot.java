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

import com.codahale.metrics.Meter;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;

/**
 * 
 */
public class MeterSnapshot extends AbstractMetricSnapshot<Meter> {

  private final long count;
  private final double meanRate;
  private final double oneMinuteRate;
  private final double fiveMinuteRate;
  private final double fifteenMinuteRate;

  MeterSnapshot(HadoopMetrics2Reporter reporter, Meter meter) {
    super(reporter);
    this.count = meter.getCount();
    this.meanRate = meter.getMeanRate();
    this.oneMinuteRate = meter.getOneMinuteRate();
    this.fiveMinuteRate = meter.getFiveMinuteRate();
    this.fifteenMinuteRate = meter.getFifteenMinuteRate();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, String baseName) {
    builder.addGauge(Interns.info(baseName + "_count", EMPTY_STRING), count);
    snapshotMeter(builder, baseName, meanRate, oneMinuteRate, fiveMinuteRate, fifteenMinuteRate);
  }
}
