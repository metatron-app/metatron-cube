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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.QueryCacheHelper;
import io.druid.query.RowResolver;
import io.druid.segment.ColumnSelectorFactories.VariableArrayIndexed;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public abstract class GenericAggregatorFactory extends AggregatorFactory.TypeResolving
{
  protected final String fieldName;
  protected final String name;
  protected final String fieldExpression;
  protected final String predicate;
  protected final ValueDesc inputType;

  protected final ValueDesc outputType;
  protected final Comparator comparator;

  public GenericAggregatorFactory(
      String name,
      String fieldName,
      String fieldExpression,
      String predicate,
      ValueDesc inputType
  )
  {
    if (name == null && (fieldName != null || fieldExpression != null)) {
      name = fieldName != null ? fieldName : Iterables.getOnlyElement(Parser.findRequiredBindings(fieldExpression));
    } else if (name != null && fieldExpression == null && fieldName == null) {
      fieldName = name;
    }
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ fieldExpression == null,
        "Must have a valid, non-null fieldName or fieldExpression"
    );
    this.inputType = inputType;
    this.name = name;
    this.fieldName = fieldName;
    this.fieldExpression = fieldExpression;
    this.predicate = predicate;
    this.outputType = inputType == null ? null : toOutputType(inputType);
    this.comparator = ValueDesc.isPrimitive(outputType) ? outputType.type().comparator() : null;
  }

  public GenericAggregatorFactory(String name, String fieldName, ValueDesc inputType)
  {
    this(name, fieldName, null, null, inputType);
  }

  @Override
  public boolean needResolving()
  {
    return inputType == null;
  }

  @Override
  public AggregatorFactory resolve(Supplier<RowResolver> resolver)
  {
    ValueDesc sourceType = resolver.get().resolve(fieldName);
    return withValue(name, fieldName, sourceType);
  }

  protected ValueDesc toOutputType(ValueDesc inputType)
  {
    return ValueDesc.isArray(inputType) ? ValueDesc.elementOfArray(inputType) : inputType;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    Preconditions.checkNotNull(inputType, "input type is not resolved");
    if (!ValueDesc.isArray(inputType)) {
      return factorize(metricFactory, inputType);
    }
    ValueDesc elementType = ValueDesc.elementOfArray(inputType);
    @SuppressWarnings("unchecked")
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(fieldName);
    final VariableArrayIndexed factory = new VariableArrayIndexed(selector, elementType);

    return new Aggregators.DelegatedAggregator(factorize(factory, elementType))
    {
      @Override
      public final void aggregate()
      {
        List values = selector.get();
        for (int i = 0; i < values.size(); i++) {
          factory.setIndex(i);
          super.aggregate();
        }
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    Preconditions.checkNotNull(inputType, "input type is not resolved");
    if (!ValueDesc.isArray(inputType)) {
      return factorizeBuffered(metricFactory, inputType);
    }
    ValueDesc elementType = ValueDesc.elementOfArray(inputType);
    @SuppressWarnings("unchecked")
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(fieldName);
    final VariableArrayIndexed factory = new VariableArrayIndexed(selector, elementType);

    return new Aggregators.DelegatedBufferAggregator(factorizeBuffered(factory, elementType))
    {
      @Override
      public final void aggregate(ByteBuffer buf, int position)
      {
        List values = selector.get();
        for (int i = 0; i < values.size(); i++) {
          factory.setIndex(i);
          super.aggregate(buf, position);
        }
      }
    };
  }

  protected abstract Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType);

  protected abstract BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueDesc valueType);

  protected abstract AggregatorFactory withValue(String name, String fieldName, ValueDesc inputType);

  protected abstract byte cacheTypeID();

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return withValue(name, name, getOutputType());
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
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      Long longValue = Longs.tryParse((String) object);
      return longValue == null ? Double.parseDouble((String) object) : longValue;
    }
    return object;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldExpression()
  {
    return fieldExpression;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @JsonProperty
  public ValueDesc getInputType()
  {
    return inputType;
  }

  @Override
  public List<String> requiredFields()
  {
    Set<String> required = Sets.newLinkedHashSet();
    if (fieldName != null) {
      required.add(fieldName);
    } else {
      required.addAll(Parser.findRequiredBindings(fieldExpression));
    }
    return Lists.newArrayList(required);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] nameBytes = StringUtils.toUtf8WithNullToEmpty(name);
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] fieldExpressionBytes = StringUtils.toUtf8WithNullToEmpty(fieldExpression);
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);
    byte[] inputTypeBytes = QueryCacheHelper.computeCacheBytes(inputType);

    int length = 1 + nameBytes.length
                   + fieldNameBytes.length
                   + fieldExpressionBytes.length
                   + predicateBytes.length
                   + inputTypeBytes.length;
    return ByteBuffer.allocate(length)
                     .put(cacheTypeID())
                     .put(nameBytes)
                     .put(fieldNameBytes)
                     .put(fieldExpressionBytes)
                     .put(predicateBytes)
                     .put(inputTypeBytes)
                     .array();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return outputType;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    switch (outputType.type()) {
      case FLOAT:
        return Floats.BYTES;
      case DOUBLE:
        return Doubles.BYTES;
      case LONG:
        return Longs.BYTES;
      case COMPLEX:
        if (outputType.isDecimal()) {
          return 128;
        }
    }
    throw new IllegalStateException();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + '{' +
           "name='" + name + '\'' +
           (fieldName == null ? "": ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "": ", fieldExpression='" + fieldExpression + '\'') +
           (predicate == null ? "": ", predicate='" + predicate + '\'') +
           ", inputType='" + inputType + '\'' +
           '}';
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

    GenericAggregatorFactory that = (GenericAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(fieldExpression, that.fieldExpression)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }
    if (!Objects.equals(inputType, that.inputType)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, fieldExpression, predicate, inputType, name);
  }
}
