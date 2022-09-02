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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactory.CubeSupport;
import io.druid.query.aggregation.AggregatorFactory.FinalizingCombinerFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.HashAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class CardinalityAggregatorFactory extends HashAggregatorFactory implements CubeSupport, FinalizingCombinerFactory
{
  public static AggregatorFactory fields(String name, List<String> fieldNames, GroupingSetSpec groupingSets)
  {
    return new CardinalityAggregatorFactory(name, fieldNames, null, groupingSets, null, true, true, 0);
  }

  public static AggregatorFactory dimensions(String name, List<DimensionSpec> dimensions, GroupingSetSpec groupingSets)
  {
    return new CardinalityAggregatorFactory(name, null, dimensions, groupingSets, null, true, true, 0);
  }

  private static final byte CACHE_TYPE_ID = (byte) 0x8;

  private final boolean round;
  private final int b;

  @JsonCreator
  public CardinalityAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("groupingSets") final GroupingSetSpec groupingSets,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("round") final boolean round,
      @JsonProperty("b") final int b
  )
  {
    super(name, predicate, fieldNames, fields, groupingSets, byRow);
    this.round = round;
    this.b = b;
    Preconditions.checkArgument(
        b == 0 || (HyperLogLogCollector.CONTEXT_START <= b && b <= HyperLogLogCollector.CONTEXT_END),
        "invalid b argument %d", b
    );
  }

  public CardinalityAggregatorFactory(String name, List<String> fieldNames, boolean byRow)
  {
    this(name, fieldNames, null, null, null, byRow, false, 0);
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @JsonProperty
  public int getB()
  {
    return b;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    List<DimensionSpec> dimensionSpecs = fields != null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = null;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new CardinalityAggregator(matcher, selectors, grouping, byRow, b);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    List<DimensionSpec> dimensionSpecs = fields != null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = null;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new CardinalityBufferAggregator(matcher, selectors, grouping, byRow, b);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.NULL_FIRST_NATURAL;
  }

  @Override
  public BinaryFn.Identical<HyperLogLogCollector> combiner()
  {
    return (param1, param2) -> param1.fold(param2);
  }

  @Override
  public Combiner.Finalizing<HyperLogLogCollector> build()
  {
    return new Combiner.Finalizing<HyperLogLogCollector>()
    {
      private final HyperLogLogCollector HLL = HyperLogLogCollector.makeLatestCollector(b);

      @Override
      public HyperLogLogCollector combine(HyperLogLogCollector param1, HyperLogLogCollector param2)
      {
        return (param1 == HLL ? HLL : HLL.overwrite(param1)).fold(param2);
      }

      @Override
      public Object finalize(HyperLogLogCollector collector)
      {
        return finalizeComputation(collector);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name, null, round, b);
  }

  @Override
  public Object deserialize(Object object)
  {
    return HyperLogLogCollector.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(object, round);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return round ? ValueDesc.LONG : ValueDesc.DOUBLE;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new HyperUniquesAggregatorFactory(name, inputField, null, round, b);
  }

  @Override
  public String getCubeName()
  {
    return b == 0 ? "cardinality" : String.format("cardinality%d", b);
  }

  @Override
  public String getFieldName()
  {
    if (fieldNames != null) {
      return Iterables.getOnlyElement(fieldNames, null);
    }
    if (fields.size() == 1 && fields.get(0) instanceof DefaultDimensionSpec) {
      return fields.get(0).getDimension();
    }
    return null;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.getCacheKey(builder.append(CACHE_TYPE_ID)).append(round).append(b);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return HyperLogLogCollector.HLL_TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.getContext(b).NUM_BYTES_FOR_DENSE_STORAGE;
  }

  @Override
  public boolean equals(Object o)
  {
    return super.equals(o) &&
           round == ((CardinalityAggregatorFactory) o).round &&
           b == ((CardinalityAggregatorFactory) o).b;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), round, b);
  }

  @Override
  public String toString()
  {
    return "CardinalityAggregatorFactory{" +
           "name='" + name + '\'' +
           (fieldNames == null ? "" : ", fieldNames=" + fieldNames) +
           (fields == null ? "" : ", fields=" + fields) +
           (groupingSets == null ? "" : ", groupingSets=" + groupingSets) +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           ", byRow=" + byRow +
           ", round=" + round +
           ", b=" + (b == 0 ? 11 : b) +
           '}';
  }
}
