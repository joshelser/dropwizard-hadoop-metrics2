/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.dropwizard.metrics.hadoop;

import static org.junit.Assert.*;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;

import java.util.Map.Entry;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the maintaining of the bounded queues of dropwizard metrics.
 */
public class BoundedQueueMaintenanceTest {

  private HadoopMetrics2Reporter reporter;

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    this.reporter = mock(HadoopMetrics2Reporter.class);

    // Call the real addEntriesToQueue method
    doCallRealMethod().when(reporter).addEntriesToQueue(any(ArrayBlockingQueue.class), any(SortedMap.class));
  }

  private void loadMap(TreeMap<String,Object> map, int numElementsToLoad) {
    final Object o = new Object();
    for (int i = 0; i < numElementsToLoad; i++) {
      map.put("metric" + i, o);
    }
  }
  
  @Test
  public void metricsInExcessOfLimitClearQueue() {
    @SuppressWarnings("unchecked")
    ArrayBlockingQueue<Entry<String,Object>> queue = mock(ArrayBlockingQueue.class);
    TreeMap<String,Object> metrics = new TreeMap<>();
    loadMap(metrics, 2);

    // Super small limit on the number of metrics we will aggregate
    when(reporter.getMaxMetricsPerType()).thenReturn(1);

    reporter.addEntriesToQueue(queue, metrics);

    verify(queue).clear();
  }

  @Test
  public void queuesAreBounded() {
    int limit = 5;
    when(reporter.getMaxMetricsPerType()).thenReturn(limit);

    ArrayBlockingQueue<Entry<String,Object>> queue = new ArrayBlockingQueue<>(limit);
    TreeMap<String,Object> metrics = new TreeMap<>();
    // [metric0, metric1, metric2]
    loadMap(metrics, 3);

    // Add three elements
    reporter.addEntriesToQueue(queue, metrics);

    // All three elements are added
    assertEquals(3, queue.size());

    // Add three elements again
    reporter.addEntriesToQueue(queue, metrics);

    // We filled up the queue
    assertEquals(limit, queue.size());

    // [0,] 1, 2, 0, 1 ,2
    assertEquals("metric1", queue.peek().getKey());

    reporter.addEntriesToQueue(queue, metrics);

    // Queue is still full
    assertEquals(5, queue.size());

    // [0, 1, 2, 0,] 1, 2, 0, 1, 2
    assertEquals("metric1", queue.peek().getKey());
  }

  @Test
  public void queuesConsumed() {
    int limit = 10;
    when(reporter.getMaxMetricsPerType()).thenReturn(limit);

    ArrayBlockingQueue<Entry<String,Object>> queue = new ArrayBlockingQueue<>(limit);
    TreeMap<String,Object> metrics = new TreeMap<>();
    loadMap(metrics, 6);

    reporter.addEntriesToQueue(queue, metrics);

    assertEquals(metrics.size(), queue.size());

    // Fake out `getMetrics()`
    // TODO refactor the reporter so that we can call the actual implementation
    Iterator<Entry<String,Object>> iter = queue.iterator();
    while (iter.hasNext()) {
      iter.next();
      // snapshot the dropwizard metric into the metrics2 metric
      iter.remove();
    }

    assertEquals(0, queue.size());

    // Add 6 entries
    reporter.addEntriesToQueue(queue, metrics);

    // Verify 6 entries
    assertEquals(metrics.size(), queue.size());

    // Add another 6 entries
    reporter.addEntriesToQueue(queue, metrics);

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
    reporter.addEntriesToQueue(queue, metrics);

    assertEquals(8, queue.size());
  }
}
