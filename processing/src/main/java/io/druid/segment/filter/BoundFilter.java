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

import com.google.common.base.Strings;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.Cacheable;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilterCacheKey;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.BitmapIndex.CumulativeSupport;

import java.util.Arrays;
import java.util.Iterator;

public class BoundFilter implements Filter
{
  private static final Logger LOG = new Logger(BoundFilter.class);

  private final BoundDimFilter boundDimFilter;

  public BoundFilter(final BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public BitmapHolder getBitmapIndex(FilterContext context)
  {
    // asserted to existing dimension
    if (boundDimFilter.isLexicographic() && boundDimFilter.getExtractionFn() == null) {
      return BitmapHolder.exact(toRangeBitmap(context));
    }
    return BitmapHolder.exact(Filters.matchPredicate(
        boundDimFilter.getDimension(),
        boundDimFilter.toPredicate(context.indexSelector()),
        context
    ));
  }

  private static final int[] ALL = new int[]{0, Integer.MAX_VALUE};
  private static final int[] NONE = new int[]{0, 0};

  // can be slower..
  private ImmutableBitmap toRangeBitmap(FilterContext context)
  {
    final BitmapIndexSelector selector = context.indexSelector();
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(boundDimFilter.getDimension());

    final int[] range = toRange(selector);
    if (range == ALL) {
      return selector.createBoolean(true);
    } else if (range == NONE || range[0] >= range[1]) {
      return selector.createBoolean(false);
    }

    LOG.debug("range.. %d ~ %d (%d)", range[0], range[1], range[1] - range[0] + 1);
    if (context.isRoot(boundDimFilter)) {
      context.range(boundDimFilter.getDimension(), RoaringBitmapFactory.from(range[0], range[1]));
    }

    if (bitmapIndex instanceof CumulativeSupport) {
      final ImmutableBitmap bitmap = tryWithCumulative((CumulativeSupport) bitmapIndex, range, context);
      if (bitmap != null) {
        return bitmap;
      }
    }
    return unionOfRange(bitmapIndex, range[0], range[1]);
  }

  // search for start, end indexes in the bitmaps; then include all bitmaps between those points
  private static ImmutableBitmap unionOfRange(final BitmapIndex bitmapIndex, final int from, final int to)
  {
    return DimFilters.union(
        bitmapIndex.getBitmapFactory(),
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              private int currIndex = from;

              @Override
              public boolean hasNext()
              {
                return currIndex < to;
              }

              @Override
              public ImmutableBitmap next()
              {
                return bitmapIndex.getBitmap(currIndex++);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  private int[] toRange(BitmapIndexSelector selector)
  {
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(boundDimFilter.getDimension());
    if (bitmapIndex == null || bitmapIndex.getCardinality() == 0) {
      return boundDimFilter.toPredicate(selector).apply(null) ? ALL : NONE;
    }
    final String lower = Strings.emptyToNull(boundDimFilter.getLower());
    final String upper = Strings.emptyToNull(boundDimFilter.getUpper());

    if (lower == null && upper == null) {
      return Strings.isNullOrEmpty(bitmapIndex.getValue(0)) ? new int[]{0, 1} : NONE;
    }
    // search for start, end indexes in the bitmaps; then include all bitmaps between those points

    final int startIndex; // inclusive
    final int endIndex; // exclusive

    if (lower == null) {
      startIndex = boundDimFilter.isLowerStrict() ? 1 : 0;
    } else {
      final int found = bitmapIndex.indexOf(lower);
      if (found >= 0) {
        startIndex = boundDimFilter.isLowerStrict() ? found + 1 : found;
      } else {
        startIndex = -(found + 1);
        if (startIndex == bitmapIndex.getCardinality()) {
          return NONE;
        }
      }
    }

    if (upper == null) {
      endIndex = bitmapIndex.getCardinality();
    } else {
      final int found = bitmapIndex.indexOf(upper);
      if (found >= 0) {
        endIndex = boundDimFilter.isUpperStrict() ? found : found + 1;
      } else {
        endIndex = -(found + 1);
        if (endIndex == 0) {
          return NONE;
        }
      }
    }
    return startIndex == 0 && endIndex == bitmapIndex.getCardinality() ? ALL : new int[]{startIndex, endIndex};
  }

  private static ImmutableBitmap tryWithCumulative(CumulativeSupport bitmap, int[] range, FilterContext context)
  {
    final int[] thresholds = bitmap.thresholds();
    if (thresholds == null || range[1] - range[0] <= thresholds[0] / 2) {
      return null;    // better to use iteration
    }
    final int numRows = context.numRows();
    final BitmapFactory factory = bitmap.getBitmapFactory();

    final ImmutableBitmap end;
    if (range[1] < bitmap.getCardinality()) {
      Cacheable key = builder -> builder.append(DimFilterCacheKey.BOUND_ROWID_CACHE_ID)
                                        .append(range[1]);
      end = context.createBitmap(key, () -> BitmapHolder.exact(getBitmapUpto(bitmap, range[1], numRows))).bitmap();
    } else {
      end = DimFilters.makeTrue(factory, context.numRows());
    }
    if (range[0] == 0) {
      return end;
    }
    Cacheable key = builder -> builder.append(DimFilterCacheKey.BOUND_ROWID_CACHE_ID)
                                      .append(range[0]);
    BitmapHolder start = context.createBitmap(key, () -> BitmapHolder.exact(getBitmapUpto(bitmap, range[0], numRows)));
    return DimFilters.difference(factory, end, start.bitmap(), context.numRows());
  }

  private static ImmutableBitmap getBitmapUpto(CumulativeSupport bitmap, int index, int numRows)
  {
    final int[] thresholds = bitmap.thresholds();
    final BitmapFactory factory = bitmap.getBitmapFactory();

    int ix = Arrays.binarySearch(thresholds, index);
    if (ix < 0) {
      ix = -ix - 1;
      final int floorIx = ix == 0 ? 0 : thresholds[ix - 1];
      final int ceilIx = ix == thresholds.length ? bitmap.getCardinality() : thresholds[ix];
      if (index - floorIx > ceilIx - index) {
        // ceil - union(endIndex .. thresholds[index])
        final ImmutableBitmap ceil = ix < thresholds.length ? bitmap.getCumulative(ix) : DimFilters.makeTrue(factory, numRows);
        final ImmutableBitmap surplus = unionOfRange(bitmap, index, ceilIx);
        final ImmutableBitmap difference = DimFilters.difference(factory, ceil, surplus, numRows);
        return difference;
      } else {
        // floor + (union(thresholds[index - 1] .. endIndex))
        final ImmutableBitmap floor = ix > 0 ? bitmap.getCumulative(ix - 1) : DimFilters.makeFalse(factory);
        final ImmutableBitmap deficit = unionOfRange(bitmap, floorIx, index);
        final ImmutableBitmap union = DimFilters.union(factory, floor, deficit);
        return union;
      }
    } else {
      return bitmap.getCumulative(ix);
    }
  }

  @Override
  public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
  {
    return Filters.toValueMatcher(factory, boundDimFilter.getDimension(), boundDimFilter.toPredicate(factory));
  }
}
