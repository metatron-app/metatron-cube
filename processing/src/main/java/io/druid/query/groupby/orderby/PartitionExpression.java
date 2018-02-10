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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class PartitionExpression implements Cacheable
{
  public static List<PartitionExpression> of(Object... values)
  {
    List<PartitionExpression> list = Lists.newArrayList();
    for (Object value : values) {
      list.add(of(value));
    }
    return list;
  }

  @JsonCreator
  public static PartitionExpression of(Object value)
  {
    if (value == null || value instanceof String) {
      return new PartitionExpression(null, (String) value);
    }
    if (value instanceof String[] && ((String[])value).length == 2) {
      return new PartitionExpression(((String[])value)[0], ((String[])value)[1]);
    }
    if (value instanceof Map) {
      Map map = (Map) value;
      return new PartitionExpression(
          Objects.toString(map.get("condition"), null),
          Objects.toString(map.get("expression"), null)
      );
    }
    throw new IllegalArgumentException("invalid argument" + value);
  }

  private final String condition;
  private final String expression;

  public PartitionExpression(String condition, String expression)
  {
    this.condition = condition;
    this.expression = Preconditions.checkNotNull(expression, "expression cannot be null");
  }

  @JsonProperty
  public String getCondition()
  {
    return condition;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] conditionBytes = QueryCacheHelper.computeCacheBytes(condition);
    byte[] expressionBytes = QueryCacheHelper.computeCacheBytes(expression);

    int length = 1
                 + conditionBytes.length
                 + expressionBytes.length;

    return ByteBuffer.allocate(length)
                     .put(conditionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(expressionBytes)
                     .array();
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

    PartitionExpression that = (PartitionExpression) o;

    if (condition != null ? !condition.equals(that.condition) : that.condition != null) {
      return false;
    }
    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = condition != null ? condition.hashCode() : 0;
    result = 31 * result + expression.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "PartitionExpression{" +
           "condition='" + condition + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }
}
