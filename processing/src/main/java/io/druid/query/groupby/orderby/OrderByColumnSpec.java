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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.common.Cacheable;
import io.druid.query.QueryCacheHelper;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.query.ordering.StringOrderingSpec;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class OrderByColumnSpec extends StringOrderingSpec implements Cacheable
{
  public static final Function<OrderByColumnSpec, String> GET_DIMENSION = new Function<OrderByColumnSpec, String>()
  {
    @Override
    public String apply(OrderByColumnSpec input)
    {
      return input.getDimension();
    }
  };

  public static List<String> getColumns(List<OrderByColumnSpec> orderByColumnSpecs)
  {
    return ImmutableList.copyOf(Lists.transform(orderByColumnSpecs, GET_DIMENSION));
  }

  public static boolean isGroupByOrdering(List<OrderByColumnSpec> orderByColumnSpecs, List<String> dimension)
  {
    int prev = Integer.MAX_VALUE;
    for (OrderByColumnSpec orderBy : orderByColumnSpecs) {
      int index = dimension.indexOf(orderBy.dimension);
      if (index < 0 || index < prev) {
        return false;
      }
      if (orderBy.direction != Direction.ASCENDING || orderBy.getComparator() != StringComparators.LEXICOGRAPHIC) {
        return false;
      }
      prev = index;
    }
    return true;
  }

  @JsonCreator
  public static OrderByColumnSpec create(Object obj)
  {
    Preconditions.checkNotNull(obj, "Cannot build an OrderByColumnSpec from a null object.");

    if (obj instanceof String) {
      return new OrderByColumnSpec(obj.toString(), null);
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final String dimension = map.get("dimension").toString();
      final Direction direction = Direction.fromString(Objects.toString(map.get("direction"), null));
      final String dimensionComparator = Objects.toString(map.get("dimensionOrder"), null);

      return new OrderByColumnSpec(dimension, direction, dimensionComparator);
    } else {
      throw new ISE("Cannot build an OrderByColumnSpec from a %s", obj.getClass());
    }
  }

  public static OrderByColumnSpec asc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.ASCENDING);
  }

  public static OrderByColumnSpec asc(String dimension, String comparator)
  {
    return new OrderByColumnSpec(dimension, Direction.ASCENDING, comparator);
  }

  public static List<OrderByColumnSpec> ascending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<String, OrderByColumnSpec>()
        {
          @Override
          public OrderByColumnSpec apply(@Nullable String input)
          {
            return asc(input);
          }
        }
    );
  }

  public static OrderByColumnSpec desc(String dimension)
  {
    return new OrderByColumnSpec(dimension, Direction.DESCENDING);
  }

  public static OrderByColumnSpec desc(String dimension, String comparator)
  {
    return new OrderByColumnSpec(dimension, Direction.DESCENDING, comparator);
  }

  public static List<OrderByColumnSpec> descending(String... dimension)
  {
    return Lists.transform(
        Arrays.asList(dimension),
        new Function<String, OrderByColumnSpec>()
        {
          @Override
          public OrderByColumnSpec apply(String input)
          {
            return desc(input);
          }
        }
    );
  }

  private final String dimension;

  public OrderByColumnSpec(String dimension, Direction direction)
  {
    this(dimension, direction, (String) null);
  }

  public OrderByColumnSpec(
      String dimension,
      Direction direction,
      StringComparator dimensionComparator
  )
  {
    this(
        dimension,
        direction,
        dimensionComparator == null ? null : dimensionComparator.toString()
    );
  }

  public OrderByColumnSpec(
      String dimension,
      Direction direction,
      String dimensionOrder
  )
  {
    super(direction, dimensionOrder);
    this.dimension = dimension;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  public OrderByColumnSpec withComparator(String comparatorName)
  {
    return new OrderByColumnSpec(dimension, direction, comparatorName);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!super.equals(o)) {
      return false;
    }
    if (!(o instanceof OrderByColumnSpec)) {
      return false;
    }
    return dimension.equals(((OrderByColumnSpec) o).dimension);
  }

  @Override
  public int hashCode()
  {
    return 31 * super.hashCode() + dimension.hashCode();
  }

  @Override
  public String toString()
  {
    return "OrderByColumnSpec{" +
           "dimension='" + dimension + '\'' +
           ", direction=" + direction + '\'' +
           ", dimensionOrder='" + dimensionOrder + '\'' +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] orderingBytes = super.getCacheKey();
    final byte[] dimensionBytes = QueryCacheHelper.computeCacheBytes(dimension);

    return ByteBuffer.allocate(orderingBytes.length + dimensionBytes.length)
                     .put(orderingBytes)
                     .put(dimensionBytes)
                     .array();
  }
}
