/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.jmx;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.guice.annotations.Self;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.segment.Segment;
import io.druid.server.DruidNode;
import org.joda.time.DateTime;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;

/**
 */
public class JMXQueryRunnerFactory extends QueryRunnerFactory.Abstract<Map<String, Object>, JMXQuery>
{
  private final DruidNode node;

  @Inject
  public JMXQueryRunnerFactory(
      @Self DruidNode node,
      JMXQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.node = node;
  }

  @Override
  public QueryRunner<Map<String, Object>> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<Map<String, Object>>()
    {
      @Override
      public Sequence<Map<String, Object>> run(Query<Map<String, Object>> query, Map<String, Object> responseContext)
      {
        Map<String, Map<String, Object>> empty = ImmutableMap.<String, Map<String, Object>>of();
        Map<String, Object> previous = query.getContextValue(Query.PREVIOUS_JMX, empty).get(node.getHostAndPort());
        return Sequences.simple(Arrays.asList(queryJMX(node, previous, ((JMXQuery) query).isDumpLongestStack())));
      }
    };
  }

  public static Map<String, Object> queryJMX(
      DruidNode node,
      Map<String, Object> previous,
      boolean dumpLongestStack
  )
  {
    MetricCollector detail = new MetricCollector(previous);
    detail.put("type", node.getType());
    detail.put("service", node.getServiceName());

    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    detail.put("startTime", new DateTime(runtimeMXBean.getStartTime()).toString());
    detail.put("inputArguments", runtimeMXBean.getInputArguments());

    OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
    detail.put("availableProcessor", osMXBean.getAvailableProcessors());
    detail.doubleMetric("systemLoadAverage", osMXBean.getSystemLoadAverage());

    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    detail.longMetric("heap.max", heapMemoryUsage.getMax());
    detail.longMetric("heap.used", heapMemoryUsage.getUsed());
    detail.longMetric("heap.committed", heapMemoryUsage.getCommitted());

    MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
    detail.longMetric("non-heap.max", nonHeapMemoryUsage.getMax());
    detail.longMetric("non-heap.used", nonHeapMemoryUsage.getUsed());
    detail.longMetric("non-heap.committed", nonHeapMemoryUsage.getCommitted());

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    detail.intMetric("threadCount", threadMXBean.getThreadCount());
    detail.intMetric("peakThreadCount", threadMXBean.getPeakThreadCount());
    detail.longMetric("totalStartedThreadCount", threadMXBean.getTotalStartedThreadCount());
    if (dumpLongestStack) {
      long current = Thread.currentThread().getId();
      ThreadInfo longest = null;
      for (long threadId : threadMXBean.getAllThreadIds()) {
        if (threadId == current) {
          continue;
        }
        ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId, 18);
        if (longest == null || threadInfo.getStackTrace().length > longest.getStackTrace().length) {
          longest = threadInfo;
        }
      }
      if (longest != null) {
        detail.put("longest.thread.name", longest.getThreadName());
        detail.put(
            "longest.thread.dump",
            Lists.newArrayList(
                Iterables.transform(
                    Arrays.asList(longest.getStackTrace()),
                    Functions.toStringFunction()
                )
            )
        );
      }
    }

    Map<String, Long> gcCollectionsCount = Maps.newHashMap();
    Map<String, Long> gcCollectionsTime = Maps.newHashMap();
    for (GarbageCollectorMXBean gcMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      gcCollectionsCount.put(gcMXBean.getName(), gcMXBean.getCollectionCount());
      gcCollectionsTime.put(gcMXBean.getName(), gcMXBean.getCollectionTime());
    }
    detail.put("gc.collectionCount", gcCollectionsCount);
    detail.put("gc.collectionTime", gcCollectionsTime);

    return detail.build(node);
  }

  private static class MetricCollector
  {
    private final Map<String, Object> current;
    private final Map<String, Object> previous;

    private MetricCollector(Map<String, Object> previous)
    {
      this.current = Maps.newLinkedHashMap();
      this.previous = previous;
    }

    private void intMetric(String metricName, int intMetric)
    {
      current.put(metricName, intMetric);
      if (previous != null && previous.containsKey(metricName)) {
        int metricDelta = intMetric - ((Number) previous.get(metricName)).intValue();
        current.put(metricName + ".delta", metricDelta);
      }
    }

    private void longMetric(String metricName, long longMetric)
    {
      current.put(metricName, longMetric);
      if (previous != null && previous.containsKey(metricName)) {
        long metricDelta = longMetric - ((Number) previous.get(metricName)).longValue();
        current.put(metricName + ".delta", metricDelta);
      }
    }

    private void doubleMetric(String metricName, double doubleMetric)
    {
      current.put(metricName, doubleMetric);
      if (previous != null && previous.containsKey(metricName)) {
        double metricDelta = doubleMetric - ((Number) previous.get(metricName)).doubleValue();
        current.put(metricName + ".delta", metricDelta);
      }
    }

    public void put(String key, Object value)
    {
      current.put(key, value);
    }

    public Map<String, Object> build(DruidNode node)
    {
      return ImmutableMap.<String, Object>of(node.getHostAndPort(), current);
    }
  }
}
