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

package io.druid.query.filter;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.math.expr.Expression;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.NotFilter;
import io.druid.segment.filter.OrFilter;

import java.util.EnumSet;
import java.util.List;

/**
 */
public interface Filter extends Expression
{
  // bitmap based filter will be applied whenever it's possible
  ImmutableBitmap getValueBitmap(BitmapIndexSelector selector);

  // bitmap based filter will be applied whenever it's possible
  ImmutableBitmap getBitmapIndex(BitmapIndexSelector selector, EnumSet<BitmapType> using);

  // used when bitmap filter cannot be applied
  ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory);

  // factory
  class Factory implements Expression.Factory<Filter>
  {
    @Override
    public Filter or(List<Filter> children)
    {
      return new OrFilter(children);
    }

    @Override
    public Filter and(List<Filter> children)
    {
      return new AndFilter(children);
    }

    @Override
    public Filter not(Filter expression)
    {
      return new NotFilter(expression);
    }
  }
}
