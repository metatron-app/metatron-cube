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

package io.druid.segment.column;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 */
public interface SecondaryIndex<T> extends Closeable
{
  default BitmapHolder filterFor(T query, FilterContext context)
  {
    return filterFor(query, context, null);
  }

  BitmapHolder filterFor(T query, FilterContext context, String attachment);

  int numRows();

  default BitmapHolder eq(String column, Comparable constant, FilterContext context)
  {
    return compare("==", false, column, constant, context);
  }

  default BitmapHolder compare(
      String op, boolean withNot, String column, Comparable constant, FilterContext context
  )
  {
    return null;
  }

  default BitmapHolder between(
      boolean withNot, String column, Comparable lower, Comparable upper, FilterContext context
  )
  {
    return null;
  }

  default BitmapHolder in(String column, List<Comparable> values, FilterContext context)
  {
    return null;
  }

  interface WithRange<T extends Comparable> extends SecondaryIndex<Range<T>>
  {
    ValueDesc type();

    @Override
    @SuppressWarnings("unchecked")
    default BitmapHolder compare(String op, boolean withNot, String column, Comparable constant, FilterContext context)
    {
      final T value = (T) constant;
      switch (op) {
        case "<":
          return filterFor(withNot ? Range.atLeast(value) : Range.lessThan(value), context);
        case ">":
          return filterFor(withNot ? Range.atMost(value) : Range.greaterThan(value), context);
        case "<=":
          return filterFor(withNot ? Range.greaterThan(value) : Range.atMost(value), context);
        case ">=":
          return filterFor(withNot ? Range.lessThan(value) : Range.atLeast(value), context);
        case "==":
          if (withNot) {
            return BitmapHolder.union(
                context.bitmapFactory(),
                Arrays.asList(
                    filterFor(Range.lessThan(value), context),
                    filterFor(Range.greaterThan(value), context)
                )
            );
          }
          return filterFor(Range.closed(value, value), context);
      }
      return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    default BitmapHolder between(
        boolean withNot, String column, Comparable lower, Comparable upper, FilterContext context
    )
    {
      if (withNot) {
        return BitmapHolder.union(
            context.bitmapFactory(),
            Arrays.asList(
                filterFor(Range.lessThan((T) lower), context),
                filterFor(Range.greaterThan((T) upper), context)
            )
        );
      }
      return filterFor(Range.closed((T) lower, (T) upper), context);
    }

    @Override
    @SuppressWarnings("unchecked")
    default BitmapHolder in(String column, List<Comparable> values, FilterContext context)
    {
      final List<BitmapHolder> holders = Lists.newArrayList();
      for (Comparable value : values) {
        holders.add(filterFor(Range.closed((T) value, (T) value), context));
      }
      return BitmapHolder.union(context.bitmapFactory(), holders);
    }
  }

  interface SupportNull<T> extends SecondaryIndex<T>
  {
    ImmutableBitmap getNulls(ImmutableBitmap baseBitmap);
  }

  interface WithRangeAndNull<T extends Comparable> extends WithRange<T>, SupportNull<Range<T>>
  {
  }
}
