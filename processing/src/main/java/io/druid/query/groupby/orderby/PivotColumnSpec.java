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
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class PivotColumnSpec extends OrderByColumnSpec
{
  public static List<PivotColumnSpec> toSpec(String... columns)
  {
    List<PivotColumnSpec> columnSpecs = Lists.newArrayList();
    for (String column : columns) {
      columnSpecs.add(create(column));
    }
    return columnSpecs;
  }

  public static List<Set<String>> getValues(List<PivotColumnSpec> pivotColumnSpecs)
  {
    List<Set<String>> comparators = Lists.newArrayList();
    for (PivotColumnSpec columnSpec : pivotColumnSpecs) {
      comparators.add(GuavaUtils.isNullOrEmpty(columnSpec.values) ? null : Sets.newHashSet(columnSpec.values));
    }
    return comparators;
  }

  public static Set[] getValuesAsArray(List<PivotColumnSpec> pivotColumnSpecs)
  {
    return getValues(pivotColumnSpecs).toArray(new Set[pivotColumnSpecs.size()]);
  }

  @JsonCreator
  public static PivotColumnSpec create(Object obj)
  {
    Preconditions.checkNotNull(obj, "Cannot build an OrderByColumnSpec from a null object.");

    if (obj instanceof String) {
      return new PivotColumnSpec(obj.toString(), null, (String) null, null);
    } else if (obj instanceof Map) {
      final Map map = (Map) obj;

      final String dimension = Preconditions.checkNotNull(map.get("dimension")).toString();
      final Direction direction = determineDirection(map.get("direction"));
      final StringComparator dimensionComparator = determineDimensionComparator(map.get("dimensionOrder"));

      return new PivotColumnSpec(dimension, direction, dimensionComparator, (List) map.get("values"));
    } else {
      throw new ISE("Cannot build an OrderByColumnSpec from a %s", obj.getClass());
    }
  }

  static StringComparator determineDimensionComparator(Object dimensionOrderObj)
  {
    if (dimensionOrderObj == null) {
      return DEFAULT_DIMENSION_ORDER;
    }

    String dimensionOrderString = dimensionOrderObj.toString().toLowerCase();
    return StringComparators.makeComparator(dimensionOrderString);
  }

  private final List<String> values;

  public PivotColumnSpec(
      String dimension,
      Direction direction,
      StringComparator dimensionComparator,
      List<String> values
  )
  {
    super(dimension, direction, dimensionComparator);
    this.values = values;
  }

  public PivotColumnSpec(
      String dimension,
      Direction direction,
      String comparatorName,
      List<String> values
  )
  {
    super(dimension, direction, comparatorName);
    this.values = values;
  }

  public PivotColumnSpec(String dimension, List<String> values)
  {
    super(dimension, null, (String) null);
    this.values = values;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @Override
  public String toString()
  {
    return "PivotColumnSpec{" +
           "dimension='" + getDimension() + '\'' +
           ", direction=" + getDirection() + '\'' +
           ", dimensionComparator='" + getDimensionComparator() + '\'' +
           ", values='" + values + '\'' +
           '}';
  }

  @Override
  public int hashCode()
  {
    return super.hashCode() * 31 + Objects.hashCode(values);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || !(o instanceof PivotColumnSpec)) {
      return false;
    }
    return super.equals(o) && Objects.equals(values, ((PivotColumnSpec) o).values);
  }
}
