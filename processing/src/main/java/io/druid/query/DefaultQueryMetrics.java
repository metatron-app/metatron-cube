/*
 * Licensed to SK Telecom Group Inc. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
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

package io.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.druid.granularity.Granularities;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DefaultQueryMetrics implements QueryMetrics
{
  protected final ObjectMapper jsonMapper;
  protected final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
  protected final Map<String, Number> metrics = new HashMap<>();

  public DefaultQueryMetrics(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void query(Query<?> query)
  {
    dataSource(query);
    queryType(query);
    interval(query);
    hasFilters(query);
    duration(query);
    queryId(query);
    granularity(query);
  }

  @Override
  public void dataSource(Query<?> query)
  {
    builder.setDimension(DruidMetrics.DATASOURCE, DataSources.getMetricName(query.getDataSource()));
  }

  @Override
  public void queryType(Query<?> query)
  {
    builder.setDimension(DruidMetrics.TYPE, query.getType());
  }

  @Override
  public void interval(Query<?> query)
  {
    builder.setDimension(
        DruidMetrics.INTERVAL,
        query.getIntervals().stream().map(Interval::toString).toArray(String[]::new)
    );
  }

  @Override
  public void hasFilters(Query<?> query)
  {
    builder.setDimension("hasFilters", String.valueOf(query.hasFilters()));
  }

  @Override
  public void duration(Query<?> query)
  {
    builder.setDimension("duration", query.getDuration().toString());
  }

  @Override
  public void queryId(Query<?> query)
  {
    builder.setDimension(DruidMetrics.ID, Strings.nullToEmpty(query.getId()));
  }

  @Override
  public void context(Query<?> query)
  {
    try {
      builder.setDimension(
          "context",
          jsonMapper.writeValueAsString(
              query.getContext() == null
              ? ImmutableMap.of()
              : query.getContext()
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void server(String host)
  {
    builder.setDimension("server", host);
  }

  @Override
  public void remoteAddress(String remoteAddress)
  {
    builder.setDimension("remoteAddress", remoteAddress);
  }

  @Override
  public void status(String status)
  {
    builder.setDimension(DruidMetrics.STATUS, status);
  }

  @Override
  public void success(boolean success)
  {
    builder.setDimension("success", String.valueOf(success));
  }

  @Override
  public void segment(String segmentIdentifier)
  {
    builder.setDimension("segment", segmentIdentifier);
  }

  @Override
  public void chunkInterval(Interval interval)
  {
    builder.setDimension("chunkInterval", interval.toString());
  }

  @Override
  public QueryMetrics reportQueryTime(long timeNs)
  {
    return defaultTimeMetric("query/time", timeNs);
  }

  @Override
  public QueryMetrics reportQueryBytes(long byteCount)
  {
    metrics.put("query/bytes", byteCount);
    return this;
  }

  @Override
  public QueryMetrics reportQueryRows(int rows)
  {
    metrics.put("query/rows", rows);
    return this;
  }

  @Override
  public QueryMetrics reportWaitTime(long timeNs)
  {
    return defaultTimeMetric("query/wait/time", timeNs);
  }

  @Override
  public QueryMetrics reportSegmentTime(long timeNs)
  {
    return defaultTimeMetric("query/segment/time", timeNs);
  }

  @Override
  public QueryMetrics reportSegmentAndCacheTime(long timeNs)
  {
    return defaultTimeMetric("query/segmentAndCache/time", timeNs);
  }

  @Override
  public QueryMetrics reportIntervalChunkTime(long timeNs)
  {
    return defaultTimeMetric("query/intervalChunk/time", timeNs);
  }

  @Override
  public QueryMetrics reportCpuTime(long timeNs)
  {
    metrics.put("query/cpu/time", TimeUnit.NANOSECONDS.toMicros(timeNs));
    return this;
  }

  @Override
  public QueryMetrics reportNodeTimeToFirstByte(long timeNs)
  {
    return defaultTimeMetric("query/node/ttfb", timeNs);
  }

  @Override
  public QueryMetrics reportNodeTime(long timeNs)
  {
    return defaultTimeMetric("query/node/time", timeNs);
  }

  private QueryMetrics defaultTimeMetric(String metricName, long timeNs)
  {
    metrics.put(metricName, TimeUnit.NANOSECONDS.toMillis(timeNs));
    return this;
  }

  @Override
  public QueryMetrics reportNodeBytes(long byteCount)
  {
    metrics.put("query/node/bytes", byteCount);
    return this;
  }

  @Override
  public void emit(ServiceEmitter emitter)
  {
    for (Map.Entry<String, Number> metric : metrics.entrySet()) {
      emitter.emit(builder.build(metric.getKey(), metric.getValue()));
    }
    metrics.clear();
  }

  @Override
  public void granularity(Query<?> query)
  {
    String serialized = Granularities.serialize(query.getGranularity(), jsonMapper);
    if (serialized != null) {
      builder.setDimension("granularity", serialized);
    }
  }
}
