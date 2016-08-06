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

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;

/**
 * 
 */
@SuppressWarnings("rawtypes")
public class GaugeSnapshot implements MetricSnapshot<Gauge> {
  private static final Logger LOG = LoggerFactory.getLogger(GaugeSnapshot.class);
  private static final String EMPTY_STRING = "";

  private final Object value;

  GaugeSnapshot(Object value) {
    this.value = value;
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, String baseName) {
    MetricsInfo info = Interns.info(baseName, EMPTY_STRING);

    // Figure out which gauge types metrics2 supports and call the right method
    if (value instanceof Integer) {
      builder.addGauge(info, (int) value);
    } else if (value instanceof Long) {
      builder.addGauge(info, (long) value);
    } else if (value instanceof Float) {
      builder.addGauge(info, (float) value);
    } else if (value instanceof Double) {
      builder.addGauge(info, (double) value);
    } else {
      LOG.trace("Ignoring Gauge ({}) with unhandled type: {}", info, value.getClass());
    }
  }
}
