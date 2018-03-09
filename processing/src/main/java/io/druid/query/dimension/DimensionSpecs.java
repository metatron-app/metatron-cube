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
import io.druid.data.ValueDesc;
import io.druid.query.Query;
import io.druid.query.RowResolver;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;

import java.util.List;

/**
 */
public class DimensionSpecs
{
  public static List<String> toInputNames(List<DimensionSpec> dimensionSpecs)
  {
    return Lists.newArrayList(Iterables.transform(dimensionSpecs, INPUT_NAME));
  }

  public static List<String> toOutputNames(List<DimensionSpec> dimensionSpecs)
  {
    return Lists.newArrayList(Iterables.transform(dimensionSpecs, OUTPUT_NAME));
  }

  public static List<ValueDesc> toOutputTypes(Query.DimensionSupport<?> query)
  {
    RowResolver resolver = RowResolver.outOf(query);
    List<ValueDesc> dimensionTypes = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      dimensionTypes.add(dimensionSpec.resolveType(resolver));
    }
    return dimensionTypes;
  }

  public static List<OrderByColumnSpec> toOrderBySpec(List<DimensionSpec> dimensionSpecs)
  {
    List<OrderByColumnSpec> orderByColumnSpecs = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : dimensionSpecs) {
      orderByColumnSpecs.add(new OrderByColumnSpec(dimensionSpec.getOutputName(), Direction.ASCENDING));
    }
    return orderByColumnSpecs;
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
}
