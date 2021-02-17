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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
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
    implements AggregatorFactory.CubeSupport
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
    this.comparator = ValueDesc.isPrimitive(outputType) ? outputType.comparator() : GuavaUtils.NULL_FIRST_NATURAL;
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
  public AggregatorFactory resolve(Supplier<? extends TypeResolver> resolver)
  {
    if (fieldName != null) {
      ValueDesc sourceType = resolver.get().resolve(fieldName);
      return withName(name, fieldName, sourceType);
    } else {
      ValueDesc sourceType = Parser.parse(fieldExpression, resolver.get()).returns();
      return withExpression(name, fieldExpression, sourceType);
    }
  }

  protected ValueDesc toOutputType(ValueDesc inputType)
  {
    return inputType.isArray() ? inputType.subElement(ValueDesc.UNKNOWN) : inputType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    Preconditions.checkNotNull(inputType, "input type is not resolved");
    if (!inputType.isArray()) {
      return factorize(metricFactory, inputType);
    }
    final ValueDesc elementType = inputType.subElement(ValueDesc.UNKNOWN);
    final ObjectColumnSelector<List> selector = metricFactory.makeObjectColumnSelector(fieldName);
    final VariableArrayIndexed factory = new VariableArrayIndexed(selector, elementType);

    return new Aggregators.DelegatedAggregator(factorize(factory, elementType))
    {
      @Override
      public final Object aggregate(Object current)
      {
        final List values = selector.get();
        if (current == null) {
          return values;
        }
        List list = (List) current;
        for (int i = 0; i < values.size(); i++) {
          factory.setIndex(i);
          list.set(i, delegate.aggregate(list.get(i)));
        }
        return list;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    Preconditions.checkNotNull(inputType, "input type is not resolved");
    if (!inputType.isArray()) {
      return factorizeBuffered(metricFactory, inputType);
    }
    ValueDesc elementType = inputType.subElement(ValueDesc.UNKNOWN);
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

  protected abstract AggregatorFactory withName(String name, String fieldName, ValueDesc inputType);

  protected abstract AggregatorFactory withExpression(String name, String fieldExpression, ValueDesc inputType);

  protected abstract byte cacheTypeID();

  @Override
  public Comparator getComparator()
  {
    return comparator;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return withName(name, name, getOutputType());
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

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return withName(name, inputField, getOutputType());
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldExpression()
  {
    return fieldExpression;
  }

  @Override
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(cacheTypeID())
                  .append(fieldName, fieldExpression, predicate)
                  .append(inputType);
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
        return Float.BYTES;
      case DOUBLE:
        return Double.BYTES;
      case LONG:
        return Long.BYTES;
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
           (fieldName == null ? "" : ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "" : ", fieldExpression='" + fieldExpression + '\'') +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           (inputType == null ? "" : ", inputType='" + inputType + '\'') +
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
