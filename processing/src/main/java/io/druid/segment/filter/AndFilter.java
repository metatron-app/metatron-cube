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

import com.google.common.collect.Lists;
import io.druid.math.expr.Expression;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;

import java.util.List;

/**
 */
public class AndFilter implements Filter, Expression.AndExpression
{
  private final List<Filter> filters;

  public AndFilter(List<Filter> filters)
  {
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
    return BitmapHolder.intersection(context.bitmapFactory(), holders);
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    if (filters.size() == 0) {
      return BooleanValueMatcher.FALSE;
    }
    final List<ValueMatcher> matchers = Lists.newArrayList();
    for (Filter filter : filters) {
      matchers.add(filter.makeMatcher(context, factory));
    }
    return makeMatcher(matchers);
  }

  public static ValueMatcher makeMatcher(final List<ValueMatcher> baseMatchers)
  {
    if (baseMatchers.size() == 0) {
      return ValueMatcher.TRUE;
    }
    if (baseMatchers.size() == 1) {
      return baseMatchers.get(0);
    }

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : baseMatchers) {
          if (!matcher.matches()) {
            return false;
          }
        }
        return true;
      }
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
    return "AND " + filters;
  }
}
