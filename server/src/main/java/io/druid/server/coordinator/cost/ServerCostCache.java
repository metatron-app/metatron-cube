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

package io.druid.server.coordinator.cost;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ServerCostCache
{
  private final SegmentsCostCache allSegmentsCostCache;
  private final Map<String, SegmentsCostCache> segmentsPerDataSource;

  ServerCostCache(
      SegmentsCostCache allSegmentsCostCache,
      Map<String, SegmentsCostCache> segmentsCostPerDataSource
  )
  {
    this.allSegmentsCostCache = Preconditions.checkNotNull(allSegmentsCostCache);
    this.segmentsPerDataSource = Preconditions.checkNotNull(segmentsCostPerDataSource);
  }

  double computeCost(DataSegment segment)
  {
    return allSegmentsCostCache.cost(segment) + computeDataSourceCost(segment);
  }

  private double computeDataSourceCost(DataSegment segment)
  {
    SegmentsCostCache costCache = segmentsPerDataSource.get(segment.getDataSource());
    return (costCache == null) ? 0.0 : costCache.cost(segment);
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private final SegmentsCostCache.Builder allSegmentsCostCache = SegmentsCostCache.builder();
    private final Map<String, SegmentsCostCache.Builder> segmentsPerDataSource = new HashMap<>();

    public Builder addSegment(final DataSegment dataSegment)
    {
      allSegmentsCostCache.addSegment(dataSegment);
      segmentsPerDataSource
          .computeIfAbsent(
              dataSegment.getDataSource(),
              new Function<String, SegmentsCostCache.Builder>()
              {
                @Override
                public SegmentsCostCache.Builder apply(String d)
                {
                  return SegmentsCostCache.builder();
                }
              }
          )
          .addSegment(dataSegment);
      return this;
    }

    public Builder removeSegment(final DataSegment dataSegment)
    {
      allSegmentsCostCache.removeSegment(dataSegment);
      segmentsPerDataSource.computeIfPresent(
          dataSegment.getDataSource(),
          new BiFunction<String, SegmentsCostCache.Builder, SegmentsCostCache.Builder>()
          {
            @Override
            public SegmentsCostCache.Builder apply(String ds, SegmentsCostCache.Builder builder)
            {
              return builder.removeSegment(dataSegment).isEmpty() ? null : builder;
            }
          }
      );
      return this;
    }

    public boolean isEmpty()
    {
      return allSegmentsCostCache.isEmpty();
    }

    public ServerCostCache build()
    {
      Map<String, SegmentsCostCache> costMap = Maps.newHashMap();
      for (Map.Entry<String, SegmentsCostCache.Builder> entry : segmentsPerDataSource.entrySet()) {
        costMap.put(entry.getKey(), entry.getValue().build());
      }
      return new ServerCostCache(allSegmentsCostCache.build(), costMap);
    }
  }
}
