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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory.CubeSupport;
import io.druid.query.aggregation.AggregatorFactory.NumericEvalSupport;
import io.druid.query.aggregation.AggregatorFactory.Vectorizable;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.ColumnStats;
import io.druid.segment.Cursor;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.QueryableIndex;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public abstract class NumericAggregatorFactory extends NumericEvalSupport implements CubeSupport, Vectorizable
{
  protected final String name;
  protected final String fieldName;
  protected final String fieldExpression;
  protected final String predicate;

  public NumericAggregatorFactory(String name, String fieldName, String fieldExpression, String predicate)
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ fieldExpression == null,
        "Must have a valid, non-null fieldName or fieldExpression"
    );

    this.name = name;
    this.fieldName = fieldName;
    this.fieldExpression = fieldExpression;
    this.predicate = predicate;
  }

  @Override
  public AggregatorFactory optimize(Cursor cursor)
  {
    if (fieldName != null && predicate == null) {
      String statsKey = statKey();
      if (statsKey != null) {
        Double constant = ColumnStats.getDouble(cursor.getStats(fieldName), statsKey);
        if (constant != null) {
          return AggregatorFactory.constant(this, constant);
        }
      }
    }
    return this;
  }

  protected String statKey()
  {
    return null;
  }

  @Override
  protected Pair<String, Object> evaluateOn()
  {
    if (fieldName != null && predicate == null) {
      return Pair.of(fieldName, nullValue());
    }
    return null;
  }

  protected Object nullValue()
  {
    return null;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
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

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    if (fieldName != null && predicate == null) {
      return Lists.newArrayList(fieldName);
    }
    Set<String> required = Sets.newLinkedHashSet();
    if (fieldName != null) {
      required.add(fieldName);
    } else {
      required.addAll(Parser.findRequiredBindings(fieldExpression));
    }
    if (predicate != null) {
      required.addAll(Parser.findRequiredBindings(predicate));
    }
    return Lists.newArrayList(required);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(cacheKey())
                  .append(fieldName, fieldExpression, predicate);
  }

  protected abstract byte cacheKey();

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "name='" + name + '\'' +
           (fieldName == null ? "" : ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "" : ", fieldExpression='" + fieldExpression + '\'') +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
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

    NumericAggregatorFactory that = (NumericAggregatorFactory) o;

    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(fieldExpression, that.fieldExpression)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, fieldExpression, predicate);
  }

  protected ValueMatcher predicateToMatcher(ColumnSelectorFactory factory)
  {
    return ColumnSelectors.toMatcher(predicate, factory);
  }

  protected static abstract class DoubleType extends NumericAggregatorFactory
  {
    public DoubleType(String name, String fieldName, String fieldExpression, String predicate)
    {
      super(name, fieldName, fieldExpression, predicate);
    }

    protected FloatColumnSelector toFloatColumnSelector(ColumnSelectorFactory metricFactory)
    {
      return ColumnSelectors.getFloatColumnSelector(metricFactory, fieldName, fieldExpression);
    }

    protected DoubleColumnSelector toDoubleColumnSelector(ColumnSelectorFactory metricFactory)
    {
      return ColumnSelectors.getDoubleColumnSelector(metricFactory, fieldName, fieldExpression);
    }

    @Override
    public ValueDesc getOutputType()
    {
      return ValueDesc.DOUBLE;
    }

    @Override
    public Comparator getComparator()
    {
      return DoubleSumAggregator.COMPARATOR;
    }

    @Override
    public Object deserialize(Object object)
    {
      // handle "NaN" / "Infinity" values serialized as strings in JSON
      if (object instanceof String) {
        return Double.parseDouble((String) object);
      }
      return object;
    }

    @Override
    public boolean supports(QueryableIndex index)
    {
      return predicate == null && fieldExpression == null &&
             DoubleColumnSelector.Scannable.class.isAssignableFrom(ColumnSelectors.asDouble(index.getGenericColumnType(fieldName)));
    }
  }

  protected static abstract class LongType extends NumericAggregatorFactory
  {
    public LongType(String name, String fieldName, String fieldExpression, String predicate)
    {
      super(name, fieldName, fieldExpression, predicate);
    }

    protected LongColumnSelector getLongColumnSelector(ColumnSelectorFactory metricFactory)
    {
      return ColumnSelectors.getLongColumnSelector(metricFactory, fieldName, fieldExpression);
    }

    @Override
    public ValueDesc getOutputType()
    {
      return ValueDesc.LONG;
    }

    @Override
    public Comparator getComparator()
    {
      return LongSumAggregator.COMPARATOR;
    }

    @Override
    public boolean supports(QueryableIndex index)
    {
      return predicate == null && fieldExpression == null &&
             LongColumnSelector.Scannable.class.isAssignableFrom(ColumnSelectors.asLong(index.getGenericColumnType(fieldName)));
    }
  }
}
