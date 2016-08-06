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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Metric;
import com.github.joshelser.dropwizard.metrics.hadoop.snapshot.MetricSnapshot;

/**
 * Test class for the maintaining of the bounded queues of dropwizard metrics.
 */
public class BoundedQueueMaintenanceTest {

  private HadoopMetrics2Reporter reporter;
  private MetricSnapshotFactory snapshotFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    this.reporter = mock(HadoopMetrics2Reporter.class);
    this.snapshotFactory = mock(MetricSnapshotFactory.class);

    // Call the real addEntriesToQueue method
    doCallRealMethod().when(reporter).addEntriesToQueue(any(ArrayBlockingQueue.class), any(SortedMap.class), any(MetricSnapshotFactory.class));
    when(reporter.consumeIncomingMetrics(any(Iterator.class), anyInt())).thenCallRealMethod();
  }

  private void loadMap(TreeMap<String,Metric> map, int numElementsToLoad) {
    for (int i = 0; i < numElementsToLoad; i++) {
      map.put("metric" + i, null);
    }
  }

  private int count(Iterator<?> iter) {
    Objects.requireNonNull(iter);
    int count = 0;
    while (iter.hasNext()) {
      count++;
      iter.next();
    }
    return count;
  }

  @Test
  public void metricsInExcessOfLimitClearQueue() {
    @SuppressWarnings("unchecked")
    ArrayBlockingQueue<Entry<String,MetricSnapshot<Metric>>> queue = mock(ArrayBlockingQueue.class);
    TreeMap<String,Metric> metrics = new TreeMap<>();
    loadMap(metrics, 2);

    // Super small limit on the number of metrics we will aggregate
    when(reporter.getMaxMetricsPerType()).thenReturn(1);

    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    verify(queue).clear();
  }

  @Test
  public void queuesAreBounded() {
    int limit = 5;
    when(reporter.getMaxMetricsPerType()).thenReturn(limit);

    ArrayBlockingQueue<Entry<String,MetricSnapshot<Metric>>> queue = new ArrayBlockingQueue<>(limit);
    TreeMap<String,Metric> metrics = new TreeMap<>();
    // [metric0, metric1, metric2]
    loadMap(metrics, 3);

    // Add three elements
    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    // All three elements are added
    assertEquals(3, queue.size());

    // Add three elements again
    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    // We filled up the queue
    assertEquals(limit, queue.size());

    // [0,] 1, 2, 0, 1 ,2
    assertEquals("metric1", queue.peek().getKey());

    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    // Queue is still full
    assertEquals(5, queue.size());

    // [0, 1, 2, 0,] 1, 2, 0, 1, 2
    assertEquals("metric1", queue.peek().getKey());
  }

  @Test
  public void queuesConsumed() {
    int limit = 10;
    when(reporter.getMaxMetricsPerType()).thenReturn(limit);

    ArrayBlockingQueue<Entry<String,MetricSnapshot<Metric>>> queue = new ArrayBlockingQueue<>(limit);
    TreeMap<String,Metric> metrics = new TreeMap<>();
    loadMap(metrics, 6);

    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    assertEquals(metrics.size(), queue.size());

    // Fake out `getMetrics()`
    // TODO refactor the reporter so that we can call the actual implementation
    Iterator<Entry<String,MetricSnapshot<Metric>>> iter = queue.iterator();
    while (iter.hasNext()) {
      iter.next();
      // snapshot the dropwizard metric into the metrics2 metric
      iter.remove();
    }

    assertEquals(0, queue.size());

    // Add 6 entries
    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    // Verify 6 entries
    assertEquals(metrics.size(), queue.size());

    // Add another 6 entries
    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    // We should have hit the limit
    assertEquals(limit, queue.size());

    // Consume 8 records
    iter = queue.iterator();
    for (int i = 0; i < 8; i++) {
      assertTrue(iter.hasNext());
      iter.next();
      // snapshot the dropwizard metric into the metrics2 metric
      iter.remove();
    }

    assertEquals(2, queue.size());

    // Add six more one final time
    reporter.addEntriesToQueue(queue, metrics, snapshotFactory);

    assertEquals(8, queue.size());
  }

  @Test
  public void incomingMetricPruning() {
    TreeMap<String,Metric> entries = new TreeMap<>();
    loadMap(entries, 10);

    Iterator<Entry<String,Metric>> iter = entries.entrySet().iterator();
    int entriesPruned = reporter.consumeIncomingMetrics(iter, 4);
    assertEquals(4, entriesPruned);
    assertEquals(6, count(iter));

    entries.clear();
    iter = entries.entrySet().iterator();
    assertEquals(0, reporter.consumeIncomingMetrics(iter, 4));
    assertEquals(0, count(iter));

    loadMap(entries, 10);
    iter = entries.entrySet().iterator();
    entriesPruned = reporter.consumeIncomingMetrics(iter, 10);
    assertEquals(10, entriesPruned);
    assertEquals(0, count(iter));

    loadMap(entries, 10);
    iter = entries.entrySet().iterator();
    entriesPruned = reporter.consumeIncomingMetrics(iter, 20);
    assertEquals(10, entriesPruned);
    assertEquals(0, count(iter));
  }
}
