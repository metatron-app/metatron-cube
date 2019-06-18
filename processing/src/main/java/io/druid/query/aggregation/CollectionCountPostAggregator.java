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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class CollectionCountPostAggregator implements PostAggregator
{
  private final String name;
  private final String field;
  private final String expression;

  @JsonIgnore private final Expr parsed;
  @JsonIgnore private final boolean identifier;
  @JsonIgnore private final List<String> dependentFields;

  @JsonCreator
  public CollectionCountPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("field") String field,
      @JsonProperty("expression") String expression
  )
  {
    Preconditions.checkArgument(
        field == null ^ expression == null, "Must have a valid, non-null field xor expression"
    );
    this.name = Preconditions.checkNotNull(name, "name should not be null");
    this.field = field;
    this.expression = expression;

    this.parsed = expression == null ? null : Parser.parse(expression);
    this.identifier = expression != null && Evals.isIdentifier(parsed);
    this.dependentFields = expression == null ? ImmutableList.of(field) : Parser.findRequiredBindings(parsed);
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getField()
  {
    return field;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.copyOf(dependentFields);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.nullFirstNatural();
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.LONG;
  }

  @Override
  public final Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    Object value;
    if (field != null) {
      value = combinedAggregators.get(field);
    } else if (identifier) {
      value = combinedAggregators.get(expression);
    } else {
      value = parsed.eval(Parser.withMap(combinedAggregators)).value();
    }
    return value instanceof Collection ? ((Collection) value).size() : null;
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

    CollectionCountPostAggregator that = (CollectionCountPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!Objects.equals(field, that.field)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + field.hashCode();
    result = 31 * result + expression.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "CollectionCountPostAggregator{" +
           "name='" + name + '\'' +
           ", field='" + field + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }
}
