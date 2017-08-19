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

package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class HyperUniquesAggregatorFactory extends AggregatorFactory
{
  public static Object estimateCardinality(Object object)
  {
    if (object == null) {
      return 0;
    }

    return ((HyperLogLogCollector) object).estimateCardinality();
  }

  private static final byte CACHE_TYPE_ID = 0x5;

  private final String name;

  private final String fieldName;
  private final String predicate;
  @JsonCreator
  public HyperUniquesAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.predicate = predicate;
  }

  public HyperUniquesAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

    if (selector == null) {
      return Aggregators.noopAggregator();
    }

    final ValueDesc valueType = selector.type();
    if ("hyperUnique".equals(valueType.typeName())) {
      return new HyperUniquesAggregator(ColumnSelectors.toPredicate(predicate, metricFactory), selector);
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
      return Aggregators.noopBufferAggregator();
    }

    final String typeName = selector.type().typeName();
    if ("hyperUnique".equals(typeName) || ValueDesc.UNKNOWN_TYPE.equals(typeName)) {
      return new HyperUniquesBufferAggregator(ColumnSelectors.toPredicate(predicate, metricFactory), selector);
    }

    throw new IAE("Incompatible type for metric[%s], expected a HyperUnique, got a %s", fieldName, typeName);
  }

  @Override
  public Comparator getComparator()
  {
    return Ordering.<HyperLogLogCollector>natural().nullsFirst();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return ((HyperLogLogCollector) lhs).fold((HyperLogLogCollector) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return HyperLogLogCollector.makeCollector(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return HyperLogLogCollector.makeCollector((ByteBuffer) object);
    } else if (object instanceof String) {
      return HyperLogLogCollector.makeCollector(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @Override

  public Object finalizeComputation(Object object)
  {
    return estimateCardinality(object);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    List<String> required = Lists.newArrayList();
    required.add(fieldName);
    if (predicate != null) {
      required.addAll(Parser.findRequiredBindings(predicate));
    }
    return required;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + predicateBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(predicateBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "hyperUnique";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.getLatestNumBytesForDenseStorage();
  }

  @Override
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
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HyperUniquesAggregatorFactory that = (HyperUniquesAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) return false;
    if (!name.equals(that.name)) return false;
    if (!Objects.equals(predicate, that.predicate)) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + Objects.hashCode(predicate);
    return result;
  }
}
