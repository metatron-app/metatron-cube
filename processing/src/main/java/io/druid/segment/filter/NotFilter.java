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

package io.druid.segment.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.math.expr.Expression;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;

import java.util.EnumSet;

/**
 */
public class NotFilter implements Filter, Expression.NotExpression
{
  public static Filter of(Filter filter)
  {
    return filter == null ? null : new NotFilter(filter);
  }

  private final Filter baseFilter;

  public NotFilter(
      Filter baseFilter
  )
  {
    this.baseFilter = baseFilter;
  }

  public Filter getBaseFilter()
  {
    return baseFilter;
  }

  @Override
  public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
  {
    ImmutableBitmap valueMap = baseFilter.getValueBitmap(selector);
    if (valueMap != null) {
      return selector.getBitmapFactory().complement(valueMap);
    }
    return null;
  }

  @Override
  public ImmutableBitmap getBitmapIndex(
      BitmapIndexSelector selector,
      EnumSet<BitmapType> using,
      ImmutableBitmap baseBitmap
  )
  {
    return selector.getBitmapFactory().complement(
        this.baseFilter.getBitmapIndex(selector, using, baseBitmap),
        selector.getNumRows()
    );
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
  {
    final ValueMatcher baseMatcher = baseFilter.makeMatcher(columnSelectorFactory);

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
}
