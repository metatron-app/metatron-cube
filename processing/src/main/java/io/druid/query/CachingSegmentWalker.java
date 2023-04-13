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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.collections.IntList;
import io.druid.common.guava.BytesRef;
import io.druid.query.spec.DenseSegmentsSpec;
import io.druid.timeline.SegmentKey;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class CachingSegmentWalker implements QuerySegmentWalker.DenseSupport
{
  private final QuerySegmentWalker segmentWalker;

  public CachingSegmentWalker(QuerySegmentWalker segmentWalker)
  {
    this.segmentWalker = segmentWalker;
  }

  @Override
  public QueryConfig getConfig()
  {
    return segmentWalker.getConfig();
  }

  @Override
  public ExecutorService getExecutor()
  {
    return segmentWalker.getExecutor();
  }

  @Override
  public ObjectMapper getMapper()
  {
    return segmentWalker.getMapper();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return segmentWalker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return segmentWalker.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, List<SegmentKey> keys, List<IntList> partitions)
  {
    if (segmentWalker instanceof DenseSupport) {
      return ((DenseSupport) segmentWalker).getQueryRunnerForSegments(query, keys, partitions);
    }
    List<SegmentDescriptor> descriptors = DenseSegmentsSpec.toDescriptors(DataSources.getName(query), keys, partitions);
    return segmentWalker.getQueryRunnerForSegments(query, descriptors);
  }

  private final Map<BytesRef, MaterializedQuery> cache = Maps.newHashMap();

  @Override
  public boolean supportsCache()
  {
    return true;
  }

  @Override
  public MaterializedQuery getMaterialized(DataSource dataSource)
  {
    byte[] key = DataSources.toCacheKey(dataSource);
    if (key != null) {
      return cache.get(BytesRef.of(key));
    }
    return null;
  }

  @Override
  public MaterializedQuery register(DataSource dataSource, MaterializedQuery materialized)
  {
    byte[] key = DataSources.toCacheKey(dataSource);
    if (key != null) {
      cache.put(BytesRef.of(key), materialized);
    }
    return materialized;
  }
}
