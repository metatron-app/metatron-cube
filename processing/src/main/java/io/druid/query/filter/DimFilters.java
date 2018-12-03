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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.filter.Filters;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DimFilters
{
  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
  }

  public static DimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static DimFilter and(List<DimFilter> filters)
  {
    List<DimFilter> list = Filters.filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new AndDimFilter(list);
  }

  public static DimFilter or(DimFilter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static DimFilter or(List<DimFilter> filters)
  {
    List<DimFilter> list = Filters.filterNull(filters);
    return list.isEmpty() ? null : list.size() == 1 ? list.get(0) : new OrDimFilter(list);
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static RegexDimFilter regex(String dimension, String pattern)
  {
    return new RegexDimFilter(dimension, pattern, null);
  }

  public static List<DimFilter> optimize(List<DimFilter> filters)
  {
    return filterNulls(
        Lists.transform(
            filters, new Function<DimFilter, DimFilter>()
            {
              @Override
              public DimFilter apply(DimFilter input)
              {
                return input.optimize();
              }
            }
        )
    );
  }

  public static List<DimFilter> filterNulls(List<DimFilter> optimized)
  {
    return Lists.newArrayList(Iterables.filter(optimized, Predicates.notNull()));
  }

  public static class NONE implements DimFilter
  {
    @Override
    public byte[] getCacheKey()
    {
      return new byte[] {0x7f, 0x00};
    }

    @Override
    public DimFilter optimize()
    {
      return this;
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler)
    {
    }

    @Override
    public Filter toFilter()
    {
      return new Filter()
      {
        @Override
        public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
        {
          return selector.getBitmapFactory().makeEmptyImmutableBitmap();
        }

        @Override
        public ImmutableBitmap getBitmapIndex(
            BitmapIndexSelector selector, EnumSet<BitmapType> using, ImmutableBitmap baseBitmap
        )
        {
          return selector.getBitmapFactory().makeEmptyImmutableBitmap();
        }

        @Override
        public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
        {
          return ValueMatcher.FALSE;
        }
      };
    }
  }

  public static class ALL implements DimFilter
  {
    @Override
    public byte[] getCacheKey()
    {
      return new byte[] {0x7f, 0x01};
    }

    @Override
    public DimFilter optimize()
    {
      return this;
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler)
    {
    }

    @Override
    public Filter toFilter()
    {
      return new Filter()
      {
        @Override
        public ImmutableBitmap getValueBitmap(BitmapIndexSelector selector)
        {
          return makeTrue(selector.getBitmapFactory(), selector.getNumRows());
        }

        @Override
        public ImmutableBitmap getBitmapIndex(
            BitmapIndexSelector selector, EnumSet<BitmapType> using, ImmutableBitmap baseBitmap
        )
        {
          return baseBitmap != null ? baseBitmap : makeTrue(selector.getBitmapFactory(), selector.getNumRows());
        }

        @Override
        public ValueMatcher makeMatcher(ColumnSelectorFactory columnSelectorFactory)
        {
          return ValueMatcher.TRUE;
        }
      };
    }
  }

  public static ImmutableBitmap makeTrue(BitmapFactory factory, int numRows)
  {
    return factory.complement(factory.makeEmptyImmutableBitmap(), numRows);
  }

  public static ImmutableBitmap makeFalse(BitmapFactory factory)
  {
    return factory.makeEmptyImmutableBitmap();
  }
}
