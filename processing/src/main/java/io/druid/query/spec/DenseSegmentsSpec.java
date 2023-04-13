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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.IntList;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QuerySegmentWalker.DenseSupport;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.SegmentKey;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

public class DenseSegmentsSpec implements QuerySegmentSpec
{
  public static QuerySegmentSpec of(String dataSource, List<SegmentDescriptor> descriptors)
  {
    TreeMap<SegmentKey, IntList> records = Maps.newTreeMap();
    for (SegmentDescriptor descriptor : descriptors) {
      records.compute(new SegmentKey(descriptor.getInterval(), descriptor.getVersion()), (interval, prev) ->
      {
        if (prev == null) {
          return IntList.of(descriptor.getPartitionNumber());
        }
        prev.add(descriptor.getPartitionNumber());
        return prev;
      });
    }
    return new DenseSegmentsSpec(
        dataSource,
        ImmutableList.copyOf(records.keySet()),
        ImmutableList.copyOf(records.values())
    );
  }

  private final String dataSource;
  private final List<SegmentKey> keys;
  private final List<IntList> partitions;

  @JsonCreator
  public DenseSegmentsSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("keys") List<SegmentKey> keys,
      @JsonProperty("partitions") List<IntList> partitions
  )
  {
    this.dataSource = dataSource;
    this.keys = keys;
    this.partitions = partitions;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<SegmentKey> getKeys()
  {
    return keys;
  }

  @JsonProperty
  public List<IntList> getPartitions()
  {
    return partitions;
  }

  @Override
  public List<Interval> getIntervals()
  {
    return GuavaUtils.transform(keys, SegmentKey::getInterval);
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    if (walker instanceof DenseSupport) {
      return ((DenseSupport) walker).getQueryRunnerForSegments(query, keys, partitions);
    }
    return walker.getQueryRunnerForSegments(query, getDescriptors());
  }

  @JsonIgnore
  public List<SegmentDescriptor> getDescriptors()
  {
    return toDescriptors(dataSource, keys, partitions);
  }

  public static List<SegmentDescriptor> toDescriptors(
      String dataSource,
      List<SegmentKey> keys,
      List<IntList> partitions
  )
  {
    List<SegmentDescriptor> descriptors = Lists.newArrayList();
    for (int i = 0; i < keys.size(); i++) {
      SegmentKey key = keys.get(i);
      IntIterator iterator = partitions.get(i).intIterator();
      while (iterator.hasNext()) {
        descriptors.add(new SegmentDescriptor(dataSource, key.getInterval(), key.getVersion(), iterator.nextInt()));
      }
    }
    return descriptors;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DenseSegmentsSpec that = (DenseSegmentsSpec) o;
    return Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(keys, that.keys) &&
           Objects.equals(partitions, that.partitions);
  }
}
