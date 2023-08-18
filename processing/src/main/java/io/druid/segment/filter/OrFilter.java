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

package io.druid.segment.filter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.math.expr.Expression;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;

import java.util.List;

/**
 */
public class OrFilter implements Filter, Expression.OrExpression
{
  private final List<Filter> filters;

  public OrFilter(List<Filter> filters)
  {
    Preconditions.checkArgument(!filters.isEmpty(), "empty OR");
    this.filters = filters;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    final List<BitmapHolder> holders = Lists.newArrayList();
    for (Filter filter : filters) {
      BitmapHolder holder = filter.getBitmapIndex(context);
      if (holder != null) {
        holders.add(holder);
      }
    }
    return BitmapHolder.union(context, holders);
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    if (filters.size() == 1) {
      return filters.get(0).makeMatcher(context, factory);
    }
    ValueMatcher[] matchers = new ValueMatcher[filters.size()];
    for (int i = 0; i < filters.size(); i++) {
      matchers[i] = filters.get(i).makeMatcher(context, factory);
    }
    return () -> {
      for (ValueMatcher matcher : matchers) {
        if (matcher.matches()) {
          return true;
        }
      }
      return false;
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Filter> getChildren()
  {
    return filters;
  }

  @Override
  public String toString()
  {
    return "OR " + filters;
  }
}
