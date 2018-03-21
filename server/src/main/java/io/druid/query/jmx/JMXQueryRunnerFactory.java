/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.jmx;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.guice.annotations.Self;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.segment.Segment;
import io.druid.server.DruidNode;
import org.joda.time.DateTime;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class JMXQueryRunnerFactory implements QueryRunnerFactory<Map<String, Object>, JMXQuery>
{
  private final DruidNode node;
  private final JMXQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public JMXQueryRunnerFactory(
      @Self DruidNode node,
      JMXQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.node = node;
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Future<Object> preFactoring(JMXQuery query, List<Segment> segments, Supplier<RowResolver> resolver, ExecutorService exec)
  {
    return null;
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

        Map<String, Object> detail = Maps.newLinkedHashMap();
        detail.put("service", node.getServiceName());

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        long heapMax = heapMemoryUsage.getMax();
        long heapUsed = heapMemoryUsage.getUsed();
        long heapCommitted = heapMemoryUsage.getCommitted();
        detail.put("heap.max", heapMax);
        if (previous != null && previous.containsKey("heap.max")) {
          detail.put("heap.max.delta", heapMax - ((Number) previous.get("heap.max")).longValue());
        }
        detail.put("heap.used", heapUsed);
        if (previous != null && previous.containsKey("heap.used")) {
          detail.put("heap.used.delta", heapMax - ((Number) previous.get("heap.used")).longValue());
        }
        detail.put("heap.committed", heapCommitted);
        if (previous != null && previous.containsKey("heap.committed")) {
          detail.put("heap.committed.delta", heapMax - ((Number) previous.get("heap.committed")).longValue());
        }

        MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
        long nonHeapMax = nonHeapMemoryUsage.getMax();
        long nonHeapUsed = nonHeapMemoryUsage.getUsed();
        long nonHeapCommitted = nonHeapMemoryUsage.getCommitted();
        detail.put("non-heap.max", nonHeapMax);
        if (previous != null && previous.containsKey("non-heap.max")) {
          detail.put("non-heap.max.delta", nonHeapMax - ((Number) previous.get("non-heap.max")).longValue());
        }
        detail.put("non-heap.used", nonHeapUsed);
        if (previous != null && previous.containsKey("non-heap.used")) {
          detail.put("non-heap.used.delta", nonHeapUsed - ((Number) previous.get("non-heap.used")).longValue());
        }
        detail.put("non-heap.committed", nonHeapCommitted);
        if (previous != null && previous.containsKey("non-heap.committed")) {
          long nonHeapCommittedDelta = nonHeapCommitted - ((Number) previous.get("non-heap.committed")).longValue();
          detail.put("non-heap.committed.delta", nonHeapCommittedDelta);
        }

        for (GarbageCollectorMXBean gcMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
          String prefix = "gc-" + gcMXBean.getName() + ".";
          detail.put(prefix + "collectionCount", gcMXBean.getCollectionCount());
          detail.put(prefix + "collectionTime", gcMXBean.getCollectionTime());
        }

        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        detail.put("startTime", new DateTime(runtimeMXBean.getStartTime()).toString());

        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        double loadAverage = osMXBean.getSystemLoadAverage();
        detail.put("systemLoadAverage", loadAverage);
        if (previous != null && previous.containsKey("systemLoadAverage")) {
          double loadAverageDelta = loadAverage - ((Number) previous.get("systemLoadAverage")).doubleValue();
          detail.put("systemLoadAverage.delta", loadAverageDelta);
        }

        Map<String, Object> row = ImmutableMap.<String, Object>of(node.getHostAndPort(), detail);
        return Sequences.simple(Arrays.asList(row));
      }
    };
  }

  @Override
  public QueryRunner<Map<String, Object>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Map<String, Object>>> queryRunners,
      final Future<Object> optimizer
  )
  {
    return new ChainedExecutionQueryRunner<Map<String, Object>>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Map<String, Object>, JMXQuery> getToolchest()
  {
    return toolChest;
  }
}
