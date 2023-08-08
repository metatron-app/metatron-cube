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

package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactory.CubeSupport;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ObjectColumnSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("hyperUnique")
public class HyperUniquesAggregatorFactory extends AggregatorFactory implements CubeSupport
{
  public static AggregatorFactory of(String name, String fieldName) {
    return new HyperUniquesAggregatorFactory(name, fieldName, null, true, 0);
  }
  public static Object estimateCardinality(Object object, boolean round)
  {
    if (object == null) {
      return round ? 0L : 0D;
    }
    if (object instanceof Number) {
      return round ? ((Number) object).longValue() : ((Number) object).doubleValue();
    }

    final HyperLogLogCollector collector = (HyperLogLogCollector) object;

    // Avoid ternary, it causes estimateCardinalityRound to be cast to double.
    if (round) {
      return collector.estimateCardinalityRound();
    } else {
      return collector.estimateCardinality();
    }
  }

  private static final byte CACHE_TYPE_ID = 0x05;

  private final String name;
  private final boolean round;

  private final String fieldName;
  private final String predicate;

  private final int b;

  @JsonCreator
  public HyperUniquesAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("round") boolean round,
      @JsonProperty("b") int b
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
    this.round = round;
    this.b = b;
    Preconditions.checkArgument(
        b == 0 || (HyperLogLogCollector.CONTEXT_START <= b && b <= HyperLogLogCollector.CONTEXT_END),
        "invalid b argument %d", b
    );
  }

  public HyperUniquesAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, false, 0);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return Aggregator.NULL;
    }

    final ValueDesc valueType = selector.type();
    if (HyperLogLogCollector.HLL_TYPE.equals(valueType)) {
      return new HyperUniquesAggregator(ColumnSelectors.toMatcher(predicate, metricFactory), selector, b);
    }

    throw new IAE(
        "Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, valueType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return BufferAggregator.NULL;
    }

    final ValueDesc type = selector.type();
    if (type.isUnknown() || HyperLogLogCollector.HLL_TYPE.equals(type)) {
      return new HyperUniquesBufferAggregator(ColumnSelectors.toMatcher(predicate, metricFactory), selector, b);
    }

    throw new IAE("Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, type);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.nullFirstNatural();
  }

  @Override
  public BinaryFn.Identical<HyperLogLogCollector> combiner()
  {
    return (param1, param2) -> param1.fold(param2);
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
    return estimateCardinality(object, round);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return round ? ValueDesc.LONG : ValueDesc.DOUBLE;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
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
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public String getCubeName()
  {
    return b == 0 ? HyperLogLogCollector.HLL_TYPE_NAME : String.format("%s%d", HyperLogLogCollector.HLL_TYPE_NAME, b);
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new HyperUniquesAggregatorFactory(name, inputField, null, round, b);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName, predicate)
                  .append(round)
                  .append(b);
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
  public String toString()
  {
    return "HyperUniquesAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           ", round=" + round +
           ", b=" + (b == 0 ? 11 : b) +
           '}';
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HyperUniquesAggregatorFactory that = (HyperUniquesAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }
    if (!Objects.equals(round, that.round)) {
      return false;
    }
    if (!Objects.equals(b, that.b)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, predicate, round, b);
  }
}
