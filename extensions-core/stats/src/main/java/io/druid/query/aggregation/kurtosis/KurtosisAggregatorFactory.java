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

package io.druid.query.aggregation.kurtosis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("kurtosis")
public class KurtosisAggregatorFactory extends AggregatorFactory
    implements AggregatorFactory.SQLSupport, AggregatorFactory.CubeSupport
{
  protected static final byte CACHE_TYPE_ID = 23;

  protected final String name;
  protected final String fieldName;
  protected final String predicate;
  protected final ValueDesc inputType;

  @JsonCreator
  public KurtosisAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") ValueDesc inputType
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName1 should not be null");
    this.predicate = predicate;
    this.inputType = inputType == null ? ValueDesc.DOUBLE : inputType;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("kurtosis", KurtosisAggregatorCollector.class);
  }

  @Override
  @JsonProperty
  public ValueDesc getInputType()
  {
    return inputType;
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
    return new KurtosisFoldingAggregatorFactory(name, inputField, predicate);
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return KurtosisAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    switch (inputType.type()) {
      case FLOAT:
      case DOUBLE:
      case LONG:
        return KurtosisAggregator.create(
            metricFactory.makeDoubleColumnSelector(fieldName),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if ("kurtosis".equals(inputType.typeName())) {
          return KurtosisAggregator.create(
              metricFactory.makeObjectColumnSelector(fieldName),
              ColumnSelectors.toMatcher(predicate, metricFactory)
          );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected numeric or kurtosis type but got a %s", fieldName, inputType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    switch (inputType.type()) {
      case FLOAT:
      case DOUBLE:
      case LONG:
        return KurtosisBufferAggregator.create(
            name,
            ColumnSelectors.asDouble(metricFactory.makeObjectColumnSelector(fieldName)),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        if ("kurtosis".equals(inputType.typeName())) {
          return KurtosisBufferAggregator.create(
              name,
              metricFactory.makeObjectColumnSelector(fieldName),
              ColumnSelectors.toMatcher(predicate, metricFactory)
          );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected numeric or kurtosis type but got a %s", fieldName, inputType
    );
  }

  @Override
  public Comparator getComparator()
  {
    return KurtosisAggregatorCollector.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner()
    {
      @Override
      public Object combine(Object param1, Object param2)
      {
        return KurtosisAggregatorCollector.combineValues(param1, param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new KurtosisFoldingAggregatorFactory(name, name, predicate);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((KurtosisAggregatorCollector) object).getKurtosis();
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return KurtosisAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return KurtosisAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return KurtosisAggregatorCollector.from(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);
    byte[] inputTypeBytes = StringUtils.toUtf8WithNullToEmpty(inputType.typeName());

    int length = 1 + fieldNameBytes.length
                   + predicateBytes.length
                   + inputTypeBytes.length;
    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(predicateBytes)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public KurtosisAggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver)
  {
    if (fieldNames.size() != 1) {
      return null;
    }
    String fieldName = Iterables.getOnlyElement(fieldNames);
    ValueDesc inputType = resolver.resolve(fieldName);
    return new KurtosisAggregatorFactory(name, fieldName, predicate, inputType);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", predicate='" + predicate + '\'' +
           ", inputType='" + inputType + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    KurtosisAggregatorFactory that = (KurtosisAggregatorFactory) o;
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }
    if (!Objects.equals(inputType, that.inputType)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hashCode(name);
    result = 31 * result + Objects.hashCode(fieldName);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + Objects.hashCode(inputType);
    return result;
  }
}
