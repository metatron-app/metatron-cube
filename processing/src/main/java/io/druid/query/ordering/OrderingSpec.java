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

package io.druid.query.ordering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.segment.serde.ComplexMetrics;

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

  public static List<Comparator<String>> getComparator(List<? extends OrderingSpec> orderByColumnSpecs)
  {
    List<Comparator<String>> comparators = Lists.newArrayList();
    for (OrderingSpec orderByColumnSpec : orderByColumnSpecs) {
      Comparator<String> comparator = StringComparators.makeComparator(orderByColumnSpec.dimensionOrder);
      if (orderByColumnSpec.direction == Direction.DESCENDING) {
        comparator = Ordering.from(comparator).reverse();
      }
      comparators.add(comparator);
    }
    return comparators;
  }

  public static boolean isAllNaturalOrdering(List<? extends OrderingSpec> orderByColumnSpecs)
  {
    List<Comparator<String>> comparators = Lists.newArrayList();
    for (OrderingSpec orderByColumnSpec : orderByColumnSpecs) {
      if (!orderByColumnSpec.isNaturalOrdering()) {
        return false;
      }
    }
    return true;
  }

  @JsonCreator
  public static OrderingSpec create(Object obj)
  {
    if (obj == null) {
      return new OrderingSpec(null, null);
    } else if (obj instanceof String) {
      final String value = Objects.toString(obj, null);
      final int index = value.lastIndexOf(':');
      if (index > 0) {
        Direction direction = Direction.tryFromString(value.substring(index + 1));
        return new OrderingSpec(direction, direction == null ? value : value.substring(0, index));
      }
      Direction direction = Direction.tryFromString(value);
      return direction == null ? new OrderingSpec(null, value) : new OrderingSpec(direction, null);
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

  public OrderingSpec(Direction direction, String dimensionOrder)
  {
    this.direction = direction == null ? Direction.ASCENDING : direction;
    this.dimensionOrder = dimensionOrder;
  }

  @JsonProperty
  public Direction getDirection()
  {
    return direction;
  }

  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public String getDimensionOrder()
  {
    return dimensionOrder;
  }

  public Comparator getComparator()
  {
    Comparator<?> comparator;
    if (dimensionOrder == null) {
      comparator = GuavaUtils.nullFirstNatural();
    } else {
      comparator = ComplexMetrics.getComparator(ValueDesc.of(dimensionOrder));
      if (comparator == null) {
        comparator = StringComparators.makeComparator(dimensionOrder);
      }
    }
    if (direction == Direction.DESCENDING) {
      comparator = Ordering.from(comparator).reverse();
    }
    return comparator;
  }

  public boolean isBasicOrdering()
  {
    return direction == Direction.ASCENDING && isNaturalOrdering();
  }

  public boolean isNaturalOrdering()
  {
    return dimensionOrder == null || dimensionOrder.equals(StringComparators.LEXICOGRAPHIC_NAME);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OrderingSpec)) {
      return false;
    }

    OrderingSpec other = (OrderingSpec) o;
    return isSameOrdering(other.direction, other.dimensionOrder);
  }

  public final boolean isSameOrdering(Direction direction, String dimensionOrder)
  {
    return this.direction == direction && Objects.equals(
        Objects.toString(this.dimensionOrder, StringComparators.LEXICOGRAPHIC_NAME),
        Objects.toString(dimensionOrder, StringComparators.LEXICOGRAPHIC_NAME)
    );
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
           "direction=" + direction +
           (dimensionOrder == null ? "" : ", dimensionOrder='" + dimensionOrder + '\'') +
           '}';
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    final String normalized = Objects.toString(dimensionOrder, StringComparators.LEXICOGRAPHIC_NAME);
    return builder.append(direction)
                  .append(normalized);
  }
}
