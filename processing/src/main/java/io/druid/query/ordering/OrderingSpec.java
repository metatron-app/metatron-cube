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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class OrderingSpec implements Cacheable
{
  public static List<OrderingSpec> toOrderingSpecs(String... specs)
  {
    List<OrderingSpec> orderingSpecs = Lists.newArrayList();
    for (String spec : specs) {
      orderingSpecs.add(new OrderingSpec(null, spec));
    }
    return orderingSpecs;
  }

  public static List<StringComparator> getComparator(List<? extends OrderingSpec> orderByColumnSpecs)
  {
    List<StringComparator> comparators = Lists.newArrayList();
    for (OrderingSpec orderByColumnSpec : orderByColumnSpecs) {
      StringComparator comparator = StringComparators.makeComparator(orderByColumnSpec.dimensionOrder);
      if (orderByColumnSpec.direction == Direction.DESCENDING) {
        comparator = StringComparators.revert(comparator);
      }
      comparators.add(comparator);
    }
    return comparators;
  }

    @JsonCreator
  public static OrderingSpec create(Object obj)
  {
    if (obj == null) {
      return new OrderingSpec(null, null);
    } if (obj instanceof String) {
      return new OrderingSpec(null, obj.toString());
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final Direction direction = Direction.fromString(Objects.toString(map.get("direction"), null));
      final String dimensionOrder = Objects.toString(map.get("dimensionOrder"), null);

      return new OrderingSpec(direction, dimensionOrder);
    } else {
      throw new ISE("Cannot build an OrderingSpec from a %s", obj.getClass());
    }
  }

  protected final Direction direction;
  protected final String dimensionOrder;

  public OrderingSpec(
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

  public Comparator getComparator()
  {
    return StringComparators.makeComparator(dimensionOrder);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !(o instanceof OrderingSpec)) {
      return false;
    }

    OrderingSpec other = (OrderingSpec) o;
    return isSameOrdering(other.direction, other.dimensionOrder);
  }

  public final boolean isSameOrdering(Direction direction, String dimensionOrder)
  {
    return this.direction == direction && Objects.equals(this.dimensionOrder, dimensionOrder);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(direction, dimensionOrder);
  }

  @Override
  public String toString()
  {
    return "OrderingSpec{" +
           "direction=" + direction + '\'' +
           ", dimensionOrder='" + dimensionOrder + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionOrderBytes = QueryCacheHelper.computeCacheBytes(dimensionOrder);

    return ByteBuffer.allocate(dimensionOrderBytes.length + 1)
                     .put((byte) direction.ordinal())
                     .put(dimensionOrderBytes)
                     .array();
  }
}
