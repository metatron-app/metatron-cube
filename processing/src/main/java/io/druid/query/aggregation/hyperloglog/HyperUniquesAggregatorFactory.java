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
import com.google.common.collect.Ordering;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
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
public class HyperUniquesAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  public static Object estimateCardinality(Object object, boolean round)
  {
    if (object == null) {
      return round ? 0L : 0D;
    }

    final HyperLogLogCollector collector = (HyperLogLogCollector) object;

    // Avoid ternary, it causes estimateCardinalityRound to be cast to double.
    if (round) {
      return collector.estimateCardinalityRound();
    } else {
      return collector.estimateCardinality();
    }
  }

  private static final byte CACHE_TYPE_ID = 0x5;

  private final String name;
  private final boolean round;

  private final String fieldName;
  private final String predicate;

  @JsonCreator
  public HyperUniquesAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("round") boolean round
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
    this.round = round;
  }

  public HyperUniquesAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, false);
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
      return new HyperUniquesAggregator(ColumnSelectors.toMatcher(predicate, metricFactory), selector);
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
      return new HyperUniquesBufferAggregator(ColumnSelectors.toMatcher(predicate, metricFactory), selector);
    }

    throw new IAE("Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, type);
  }

  @Override
  public Comparator getComparator()
  {
    return Ordering.<HyperLogLogCollector>natural().nullsFirst();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner.Abstract<HyperLogLogCollector>()
    {
      @Override
      protected final HyperLogLogCollector _combine(HyperLogLogCollector param1, HyperLogLogCollector param2)
      {
        return param1.fold(param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name, null, round);
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
  public String getCubeName()
  {
    return "hyperUnique";
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new HyperUniquesAggregatorFactory(name, inputField, null, round);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName, predicate)
                  .append(round);
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @Override
  public ValueDesc getOutputType()
  {
    return HyperLogLogCollector.HLL_TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.NUM_BYTES_FOR_DENSE_STORAGE;
  }

  public Object getAggregatorStartValue()
  {
    return HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public String toString()
  {
    return "HyperUniquesAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", predicate='" + predicate + '\'' +
           ", round=" + round +
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

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, predicate, round);
  }
}
