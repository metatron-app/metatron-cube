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

package io.druid.segment;

import io.druid.java.util.common.ISE;
import io.druid.query.Query;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DictionaryID;
import io.druid.query.spec.SpecificSegmentSpec;

import java.util.List;

public class Segments
{
  public static Segment withLimit(Segment segment, SegmentDescriptor descriptor)
  {
    if (descriptor.getInterval().contains(segment.getInterval())) {
      return segment;
    }
    return new WithLimit(segment, descriptor);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Segment> T unwrap(Segment segment, Class<T> clazz)
  {
    if (clazz.isInstance(segment)) {
      return (T) segment;
    }
    if (segment instanceof Segment.Delegated) {
      return unwrap(((Segment.Delegated) segment).getDelegated(), clazz);
    }
    throw new ISE("Cannot find %s from %s", clazz, segment.getClass());
  }

  @SuppressWarnings("unchecked")
  public static <T extends Query> T prepare(T query, Segment segment)
  {
    return (T) query.withQuerySegmentSpec(segment.asSpec());
  }

  public static class WithLimit extends Segment.Delegated
  {
    private final SegmentDescriptor descriptor;

    public WithLimit(Segment segment, SegmentDescriptor descriptor)
    {
      super(segment);
      this.descriptor = descriptor;
    }

    @Override
    public SpecificSegmentSpec asSpec()
    {
      return new SpecificSegmentSpec(descriptor);
    }

    @Override
    public String toString()
    {
      return descriptor.toString();
    }
  }

  public static boolean isAllIndexedSingleValuedDimensions(List<Segment> segments, List<String> dimensions)
  {
    if (segments.stream().allMatch(s -> s.isIndexed())) {
      for (Segment segment : segments) {
        StorageAdapter adapter = segment.asStorageAdapter(false);
        return dimensions.stream().allMatch(
            d -> adapter.getColumnCapabilities(d) != null && !adapter.getColumnCapabilities(d).hasMultipleValues()
        );
      }
    }
    return false;
  }

  public static boolean isVectorizableDimensions(List<Segment> segments, List<String> dimensions)
  {
    for (Segment segment : segments) {
      StorageAdapter adapter = segment.asStorageAdapter(false);
      long bits = dimensions.stream().mapToInt(d -> DictionaryID.bitsRequired(adapter.getDimensionCardinality(d))).sum();
      if (bits >= Long.SIZE) {
        return false;
      }
    }
    return true;
  }

  public static boolean isVectorizableFactories(List<Segment> segments, List<AggregatorFactory> factories)
  {
    for (Segment segment : segments) {
      QueryableIndex index = segment.asQueryableIndex(false);
      if (factories.stream().map(f -> ((AggregatorFactory.Vectorizable) f)).anyMatch(v -> !v.supports(index))) {
        return false;
      }
    }
    return true;
  }
}
