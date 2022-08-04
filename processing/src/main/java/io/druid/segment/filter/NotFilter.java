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

import io.druid.math.expr.Expression;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Arrays;
import java.util.List;

/**
 */
public class NotFilter implements Filter, Expression.NotExpression
{
  public static Filter of(Filter filter)
  {
    return filter == null ? null : new NotFilter(filter);
  }

  private final Filter baseFilter;

  public NotFilter(Filter baseFilter)
  {
    this.baseFilter = baseFilter;
  }

  public Filter getBaseFilter()
  {
    return baseFilter;
  }

  @Override
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    BitmapHolder holder = baseFilter.getBitmapIndex(context);
    if (holder != null) {
      return BitmapHolder.not(context.bitmapFactory(), holder, context.numRows());
    }
    return null;
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(context, factory);

    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return !baseMatcher.matches();
      }
    };
  }

  @Override
  public String toString()
  {
    return "NOT " + baseFilter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Filter getChild()
  {
    return baseFilter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Filter> getChildren()
  {
    return Arrays.asList(baseFilter);
  }
}
