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

package io.druid.query.ordering;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 */
public class StringOrderingSpec implements Cacheable
{
  public static List<StringComparator> getComparator(List<? extends StringOrderingSpec> orderByColumnSpecs)
  {
    List<StringComparator> comparators = Lists.newArrayList();
    for (StringOrderingSpec orderByColumnSpec : orderByColumnSpecs) {
      StringComparator comparator = StringComparators.makeComparator(orderByColumnSpec.dimensionOrder);
      if (orderByColumnSpec.direction == Direction.DESCENDING) {
        comparator = StringComparators.revert(comparator);
      }
      comparators.add(comparator);
    }
    return comparators;
  }

  protected final Direction direction;
  protected final String dimensionOrder;

  public StringOrderingSpec(
      Direction direction,
      String dimensionOrder
  )
  {
    this.direction = direction == null ? Direction.ASCENDING : direction;
    this.dimensionOrder = dimensionOrder == null ? StringComparators.LEXICOGRAPHIC_NAME : dimensionOrder;
  }

  @JsonProperty
  public Direction getDirection()
  {
    return direction;
  }

  @JsonProperty
  public String getDimensionOrder()
  {
    return dimensionOrder;
  }

  public StringComparator getComparator()
  {
    return StringComparators.makeComparator(dimensionOrder);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof StringOrderingSpec)) {
      return false;
    }

    StringOrderingSpec that = (StringOrderingSpec) o;

    if (!Objects.equals(dimensionOrder, that.dimensionOrder)) {
      return false;
    }
    return direction == that.direction;

  }

  @Override
  public int hashCode()
  {
    return Objects.hash(direction, dimensionOrder);
  }

  @Override
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "direction=" + direction + '\'' +
           ", dimensionOrder='" + dimensionOrder + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionOrderBytes = QueryCacheHelper.computeCacheBytes(dimensionOrder);

    return ByteBuffer.allocate(dimensionOrderBytes.length + 1)
                     .put(dimensionOrderBytes)
                     .put((byte) direction.ordinal())
                     .array();
  }
}
