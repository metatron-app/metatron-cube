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

package io.druid.query.aggregation.countmin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("countMin")
public class CountMinAggregatorFactory extends HashAggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x26;

  private final int width;
  private final int depth;
  private final boolean combine;

  @JsonCreator
  public CountMinAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("groupingSets") final GroupingSetSpec groupingSets,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("width") final int width,
      @JsonProperty("depth") final int depth,
      @JsonProperty("combine") final boolean combine
  )
  {
    super(name, predicate, fieldNames, fields, groupingSets, byRow);
    this.width = width;
    this.depth = depth;
    this.combine = combine;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
    Preconditions.checkArgument(width > 0 && depth > 0);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      return Aggregators.asAggregator(
          combiner(), columnFactory.makeObjectColumnSelector(Iterables.getOnlyElement(fieldNames))
      );
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = GroupingSetSpec.EMPTY_INDEX;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new CountMinAggregator(matcher, selectors, grouping, byRow, width, depth);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      return CountMinBufferAggregator.combiner(
          columnFactory.makeObjectColumnSelector(Iterables.getOnlyElement(fieldNames)), width, depth
      );
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = GroupingSetSpec.EMPTY_INDEX;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    final ValueMatcher predicate = ColumnSelectors.toMatcher(this.predicate, columnFactory);
    return new CountMinBufferAggregator(predicate, selectors, grouping, byRow, width, depth);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<CountMinSketch>()
    {
      @Override
      public int compare(CountMinSketch lhs, CountMinSketch rhs)
      {
        return lhs.compareTo(rhs);
      }
    };
  }

  @Override
  public BinaryFn.Identical<CountMinSketch> combiner()
  {
    return (p1, p2) -> p1.merge(p2);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CountMinAggregatorFactory(name, Arrays.asList(name), null, null, null, false, width, depth, true);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof CountMinSketch) {
      return object;
    }
    final byte[] buffer;
    if (object instanceof byte[]) {
      buffer = (byte[]) object;
    } else if (object instanceof String) {
      buffer = StringUtils.decodeBase64((String) object);
    } else {
      throw new ISE("?? %s", object.getClass().getSimpleName());
    }
    return CountMinSketch.fromCompressedBytes(buffer);
  }

  @JsonProperty
  public int getWidth()
  {
    return width;
  }

  @JsonProperty
  public int getDepth()
  {
    return depth;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.getCacheKey(builder.append(CACHE_TYPE_ID))
                .append(width)
                .append(depth);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("countMin");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return (width * depth + 2) * Integer.BYTES;
  }

  @Override
  public boolean equals(Object o)
  {
    return super.equals(o) &&
           width == ((CountMinAggregatorFactory) o).width &&
           depth == ((CountMinAggregatorFactory) o).depth;
  }

  @Override
  public int hashCode()
  {
    return 31 * (31 * super.hashCode() + width) + depth;
  }

  @Override
  public String toString()
  {
    return "CountMinAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames='" + fieldNames + '\'' +
           ", fields=" + fields +
           ", groupingSets=" + groupingSets +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           ", byRow=" + byRow +
           ", width=" + width +
           ", depth=" + depth +
           '}';
  }
}
