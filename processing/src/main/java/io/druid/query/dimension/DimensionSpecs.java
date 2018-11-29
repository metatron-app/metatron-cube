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

package io.druid.query.dimension;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.ordering.StringComparators;

import java.util.Comparator;
import java.util.List;

/**
 */
public class DimensionSpecs
{
  public static List<String> toInputNames(Iterable<DimensionSpec> dimensionSpecs)
  {
    return Lists.newArrayList(Iterables.transform(dimensionSpecs, INPUT_NAME));
  }

  public static List<String> toOutputNames(Iterable<DimensionSpec> dimensionSpecs)
  {
    return Lists.newArrayList(Iterables.transform(dimensionSpecs, OUTPUT_NAME));
  }

  public static List<String> toDescriptions(Iterable<DimensionSpec> dimensionSpecs)
  {
    return Lists.newArrayList(Iterables.transform(dimensionSpecs, DESCRIPTION));
  }

  public static String[] toOutputNamesAsArray(Iterable<DimensionSpec> dimensionSpecs)
  {
    return toOutputNames(dimensionSpecs).toArray(new String[0]);
  }

  public static List<ValueDesc> toOutputTypes(Query.DimensionSupport<?> query)
  {
    RowResolver resolver = RowResolver.outOf(query);
    List<ValueDesc> dimensionTypes = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      dimensionTypes.add(dimensionSpec.resolve(resolver));
    }
    return dimensionTypes;
  }

  public static List<OrderByColumnSpec> asOrderByColumnSpec(List<DimensionSpec> dimensionSpecs)
  {
    List<OrderByColumnSpec> orderingSpecs = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      if (dimensionSpec instanceof DimensionSpecWithOrdering) {
        orderingSpecs.add(((DimensionSpecWithOrdering)dimensionSpec).asOrderByColumnSpec());
      } else {
        orderingSpecs.add(OrderByColumnSpec.asc(dimensionSpec.getOutputName()));
      }
    }
    return orderingSpecs;
  }

  public static Comparator[] toComparator(List<DimensionSpec> dimensionSpecs)
  {
    return toComparator(dimensionSpecs, false);
  }

  public static Comparator[] toComparator(List<DimensionSpec> dimensionSpecs, boolean prependTime)
  {
    List<Comparator> comparators = Lists.newArrayList();
    if (prependTime) {
      comparators.add(GuavaUtils.noNullableNatural());
    }
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      comparators.add(toComparator(dimensionSpec));
    }
    return comparators.toArray(new Comparator[comparators.size()]);
  }

  public static Comparator toComparator(DimensionSpec dimensionSpec)
  {
    Comparator comparator = GuavaUtils.nullFirstNatural();
    if (dimensionSpec instanceof DimensionSpecWithOrdering) {
      OrderingSpec orderingSpec = ((DimensionSpecWithOrdering) dimensionSpec).asOrderingSpec();
      if (!orderingSpec.isNaturalOrdering()) {
        comparator = StringComparators.makeComparator(orderingSpec.getDimensionOrder());
      }
      if (orderingSpec.getDirection() == Direction.DESCENDING) {
        comparator = Ordering.from(comparator).reverse();
      }
    }
    return comparator;
  }

  private static boolean isAllDefaultOrdering(List<DimensionSpec> dimensionSpecs)
  {
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      if (!isDefaultOrdering(dimensionSpec)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isDefaultOrdering(DimensionSpec dimensionSpec)
  {
    if (dimensionSpec instanceof DimensionSpecWithOrdering) {
      return ((DimensionSpecWithOrdering) dimensionSpec).asOrderingSpec().isBasicOrdering();
    }
    return true;
  }

  public static List<Direction> getDirections(List<DimensionSpec> dimensionSpecs)
  {
    List<Direction> directions = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      if (dimensionSpec instanceof DimensionSpecWithOrdering) {
        directions.add(((DimensionSpecWithOrdering)dimensionSpec).getDirection());
      } else{
        directions.add(Direction.ASCENDING);
      }
    }
    return directions;
  }

  public static final Function<DimensionSpec, String> INPUT_NAME = new Function<DimensionSpec, String>()
  {
    @Override
    public String apply(DimensionSpec input)
    {
      return input.getDimension();
    }
  };

  public static final Function<DimensionSpec, String> OUTPUT_NAME = new Function<DimensionSpec, String>()
  {
    @Override
    public String apply(DimensionSpec input)
    {
      return input.getOutputName();
    }
  };

  public static final Function<DimensionSpec, String> DESCRIPTION = new Function<DimensionSpec, String>()
  {
    @Override
    public String apply(DimensionSpec input)
    {
      return input.getDescription();
    }
  };

  public static DimensionSpec of(String dimensionName, ExtractionFn extractionFn)
  {
    if (extractionFn != null) {
      return ExtractionDimensionSpec.of(dimensionName, extractionFn);
    }
    return DefaultDimensionSpec.of(dimensionName);
  }

  public static boolean containsExtractFn(List<DimensionSpec> dimensionSpecs)
  {
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      if (dimensionSpec.getExtractionFn() != null) {
        return true;
      }
    }
    return false;
  }

  public static boolean isAllDefault(List<DimensionSpec> dimensionSpecs)
  {
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      if (!(dimensionSpec instanceof DefaultDimensionSpec)) {
        return false;
      }
    }
    return true;
  }
}
