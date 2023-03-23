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

package io.druid.sql.calcite.filtration;

import io.druid.data.TypeResolver;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.SimpleExtraction;

import java.util.Objects;

public class ConvertBoundsToSelectors extends BottomUpTransform
{
  public static ConvertBoundsToSelectors create(TypeResolver resolver)
  {
    return new ConvertBoundsToSelectors(resolver);
  }

  private final TypeResolver resolver;

  private ConvertBoundsToSelectors(TypeResolver resolver)
  {
    this.resolver = resolver;
  }

  @Override
  public DimFilter process(DimFilter filter)
  {
    if (filter instanceof BoundDimFilter) {
      final BoundDimFilter bound = (BoundDimFilter) filter;
      final String comporatorType = Utils.comparatorFor(
          resolver, SimpleExtraction.of(bound.getDimension(), bound.getExtractionFn())
      );
      if (bound.isEquals() && Objects.equals(comporatorType, bound.getComparatorType())) {
        return new SelectorDimFilter(
            bound.getDimension(),
            bound.getUpper(),
            bound.getExtractionFn()
        );
      }
    }
    return filter;
  }
}
