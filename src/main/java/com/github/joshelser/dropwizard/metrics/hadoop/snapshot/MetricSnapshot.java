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

import com.codahale.metrics.Metric;

/**
 * Interface for point-in-time views on a {@link Metric}.
 */
public interface MetricSnapshot<T extends Metric> {

  /**
   * Adds the given snapshot of the metric to the record builder.
   *
   * @param builder The builder to add this metric to
   * @param baseName A base name for the metric record
   */
  void snapshot(MetricsRecordBuilder builder, String baseName);

}
