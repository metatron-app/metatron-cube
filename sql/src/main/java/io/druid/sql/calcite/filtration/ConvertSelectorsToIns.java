/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import io.druid.data.TypeResolver;
import io.druid.java.util.common.ISE;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.SimpleExtraction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertSelectorsToIns extends BottomUpTransform
{
  public static ConvertSelectorsToIns create(TypeResolver resolver)
  {
    return new ConvertSelectorsToIns(resolver);
  }

  private final TypeResolver resolver;

  private ConvertSelectorsToIns(TypeResolver resolver)
  {
    this.resolver = resolver;
  }

  @Override
  public DimFilter process(DimFilter filter)
  {
    if (filter instanceof OrDimFilter) {
      // Copy children list
      final List<DimFilter> children = Lists.newArrayList(((OrDimFilter) filter).getFields());

      // Group filters by dimension and extractionFn.
      final Map<BoundRefKey, List<SelectorDimFilter>> selectors = new HashMap<>();

      for (DimFilter child : children) {
        if (child instanceof SelectorDimFilter) {
          final SelectorDimFilter selector = (SelectorDimFilter) child;
          final BoundRefKey boundRefKey = BoundRefKey.from(
              selector,
              Utils.comparatorFor(resolver, SimpleExtraction.of(selector.getDimension(), selector.getExtractionFn()))
          );
          List<SelectorDimFilter> filterList = selectors.get(boundRefKey);
          if (filterList == null) {
            filterList = new ArrayList<>();
            selectors.put(boundRefKey, filterList);
          }
          filterList.add(selector);
        }
      }

      // Emit IN filters for each group of size > 1.
      for (Map.Entry<BoundRefKey, List<SelectorDimFilter>> entry : selectors.entrySet()) {
        final List<SelectorDimFilter> filterList = entry.getValue();
        if (filterList.size() > 1) {
          // We found a simplification. Remove the old filters and add new ones.
          final List<String> values = new ArrayList<>();

          for (final SelectorDimFilter selector : filterList) {
            values.add(selector.getValue());
            if (!children.remove(selector)) {
              throw new ISE("Tried to remove selector but couldn't?");
            }
          }

          children.add(InDimFilter.of(entry.getKey().getDimension(), values, entry.getKey().getExtractionFn()));
        }
      }

      if (!children.equals(((OrDimFilter) filter).getFields())) {
        return children.size() == 1 ? children.get(0) : new OrDimFilter(children);
      } else {
        return filter;
      }
    } else {
      return filter;
    }
  }
}
