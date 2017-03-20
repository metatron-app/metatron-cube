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

package io.druid.query.aggregation.corr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueType;
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
@JsonTypeName("pearson")
public class PearsonAggregatorFactory extends AggregatorFactory
{
  protected static final byte CACHE_TYPE_ID = 20;

  protected final String name;
  protected final String fieldName1;
  protected final String fieldName2;
  protected final String predicate;
  protected final String inputType;

  @JsonIgnore
  protected final ValueType valueType;

  @JsonCreator
  public PearsonAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName1") String fieldName1,
      @JsonProperty("fieldName2") String fieldName2,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") String inputType
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName1 = Preconditions.checkNotNull(fieldName1, "fieldName1 should not be null");
    this.fieldName2 = fieldName2;
    this.predicate = predicate;
    this.inputType = inputType == null ? "double" : inputType;
    this.valueType = ValueType.of(this.inputType);
    if (ValueType.isNumeric(valueType)) {
      Preconditions.checkArgument(fieldName2 != null, "fieldName2 should not be null");
    } else {
      Preconditions.checkArgument(fieldName2 == null, "fieldName2 should be null");
    }
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
    return valueType == ValueType.COMPLEX ? Arrays.asList(fieldName1) : Arrays.asList(fieldName1, fieldName2);
  }

  @Override
  public String getTypeName()
  {
    return "pearson";
  }

  @Override
  @JsonProperty
  public String getInputTypeName()
  {
    return inputType;
  }

  @JsonProperty
  public String getFieldName1()
  {
    return fieldName1;
  }

  @JsonProperty
  public String getFieldName2()
  {
    return fieldName2;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return PearsonAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case LONG:
        return PearsonAggregator.create(
            name,
            metricFactory.makeDoubleColumnSelector(fieldName1),
            metricFactory.makeDoubleColumnSelector(fieldName2),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case COMPLEX:
        if ("pearson".equalsIgnoreCase(inputType)) {
          return PearsonAggregator.create(
              name,
              metricFactory.makeObjectColumnSelector(fieldName1),
              ColumnSelectors.toPredicate(predicate, metricFactory)
          );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected numeric or pearson type but got a %s", fieldName1, inputType
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    switch (valueType) {
      case FLOAT:
      case DOUBLE:
      case LONG:
        return PearsonBufferAggregator.create(
            name,
            ColumnSelectors.asDouble(metricFactory.makeObjectColumnSelector(fieldName1)),
            ColumnSelectors.asDouble(metricFactory.makeObjectColumnSelector(fieldName2)),
            ColumnSelectors.toPredicate(predicate, metricFactory)
        );
      case COMPLEX:
        if ("pearson".equalsIgnoreCase(inputType)) {
          return PearsonBufferAggregator.create(
              name,
              metricFactory.makeObjectColumnSelector(fieldName1),
              ColumnSelectors.toPredicate(predicate, metricFactory)
          );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected numeric or pearson type but got a %s", fieldName1, inputType
    );
  }

  @Override
  public Comparator getComparator()
  {
    return PearsonAggregatorCollector.COMPARATOR;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return new PearsonAggregatorCollector();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return PearsonAggregatorCollector.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new PearsonFoldingAggregatorFactory(name, name, predicate);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new PearsonFoldingAggregatorFactory(fieldName1, fieldName1, predicate));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return ((PearsonAggregatorCollector) object).getCorr();
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return PearsonAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return PearsonAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return PearsonAggregatorCollector.from(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] nameBytes = StringUtils.toUtf8WithNullToEmpty(name);
    byte[] fieldName1Bytes = StringUtils.toUtf8WithNullToEmpty(fieldName1);
    byte[] fieldName2Bytes = StringUtils.toUtf8WithNullToEmpty(fieldName2);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);
    byte[] inputTypeBytes = StringUtils.toUtf8WithNullToEmpty(inputType);

    int length = 1 + nameBytes.length
                   + fieldName1Bytes.length
                   + fieldName2Bytes.length
                   + predicateBytes.length
                   + inputTypeBytes.length;
    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(nameBytes)
                     .put(fieldName1Bytes)
                     .put(fieldName2Bytes)
                     .put(predicateBytes)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           ", fieldName1='" + fieldName1 + '\'' +
           ", fieldName2='" + fieldName2 + '\'' +
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
    PearsonAggregatorFactory that = (PearsonAggregatorFactory) o;
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName1, that.fieldName1)) {
      return false;
    }
    if (!Objects.equals(fieldName2, that.fieldName2)) {
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
    result = 31 * result + Objects.hashCode(fieldName1);
    result = 31 * result + Objects.hashCode(fieldName2);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + Objects.hashCode(inputType);
    return result;
  }
}
