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

package io.druid.query.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.BoundType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.WrappedConciseBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Ranges;
import io.druid.data.Pair;
import io.druid.data.TypeResolver;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expressions;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.ViewDataSource;
import io.druid.query.filter.DimFilter.Compressed;
import io.druid.query.filter.DimFilter.Compressible;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.VirtualColumn;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.WrappedBitSetBitmap;
import io.druid.segment.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.MatcherContext;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class DimFilters
{
  private static final Logger LOG = new Logger(DimFilters.class);
  private static final Expression.Factory<DimFilter> FACTORY = new DimFilter.Factory();

  public static SelectorDimFilter dimEquals(String dimension, String value)
  {
    return new SelectorDimFilter(dimension, value, null);
  }

  private static List<DimFilter> merge(List<DimFilter> fields, DimFilter.OP op)
  {
    if (fields.size() <= 1) {
      return fields;
    }
    final Iterable<DimFilter.Mergeable> mergeables = Iterables.filter(fields, DimFilter.Mergeable.class);
    if (Iterables.isEmpty(mergeables)) {
      return fields;
    }
    boolean anyMerge = false;
    final Set<DimFilter> remains = Sets.newHashSet(fields);
    for (DimFilter.Mergeable mergeable : mergeables) {
      if (!remains.remove(mergeable)) {
        continue;   // merged already
      }
      Iterator<DimFilter> iterator = remains.iterator();
      while (iterator.hasNext()) {
        DimFilter filter = iterator.next();
        if (mergeable.supports(op, filter)) {
          anyMerge = true;
          iterator.remove();
          DimFilter merged = mergeable.merge(op, filter);
          if (merged instanceof DimFilter.Mergeable) {
            mergeable = (DimFilter.Mergeable) merged;
          } else {
            remains.add(merged);
            break;
          }
        }
      }
      remains.add(mergeable);
    }
    return anyMerge ? Lists.newArrayList(remains) : fields;
  }

  public static DimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static DimFilter and(List<DimFilter> filters)
  {
    if (filters.contains(NONE)) {
      return NONE;
    }
    List<DimFilter> list = Lists.newArrayList();
    for (DimFilter filtered : Iterables.filter(filters, p -> p != null && !ALL.equals(p))) {
      if (filtered instanceof AndDimFilter) {
        list.addAll(((AndDimFilter) filtered).getChildren());
      } else {
        list.add(filtered);
      }
    }
    List<DimFilter> merged = merge(list, DimFilter.OP.AND);
    return merged.isEmpty() ? null : merged.size() == 1 ? merged.get(0) : new AndDimFilter(merged);
  }

  public static <T> Query.FilterSupport<T> and(Query.FilterSupport<T> query, DimFilter filter)
  {
    return query.withFilter(and(query.getFilter(), filter));
  }

  public static DimFilter or(DimFilter... filters)
  {
    return or(Arrays.asList(filters));
  }

  public static DimFilter or(List<DimFilter> filters)
  {
    if (filters.contains(ALL)) {
      return ALL;
    }
    List<DimFilter> list = Lists.newArrayList();
    for (DimFilter filtered : Iterables.filter(filters, p -> p != null && !NONE.equals(p))) {
      if (filtered instanceof OrDimFilter) {
        list.addAll(((OrDimFilter) filtered).getChildren());
      } else {
        list.add(filtered);
      }
    }
    List<DimFilter> merged = merge(list, DimFilter.OP.OR);
    return merged.isEmpty() ? null : merged.size() == 1 ? merged.get(0) : new OrDimFilter(merged);
  }

  public static NotDimFilter not(DimFilter filter)
  {
    return new NotDimFilter(filter);
  }

  public static List<DimFilter> filterNulls(List<DimFilter> optimized)
  {
    return Lists.newArrayList(Iterables.filter(optimized, Predicates.notNull()));
  }

  public static DimFilter convertToCNF(DimFilter current)
  {
    return current == null ? null : Expressions.convertToCNF(current, FACTORY);
  }

  public static Pair<ImmutableBitmap, DimFilter> extractBitmaps(DimFilter current, FilterContext context)
  {
    if (current == null) {
      return Pair.<ImmutableBitmap, DimFilter>of(null, null);
    }
    if (current instanceof AndDimFilter) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      List<DimFilter> remainings = Lists.newArrayList();
      for (DimFilter child : ((AndDimFilter) current).getChildren()) {
        BitmapHolder holder = Filters.toBitmapHolder(child, context);
        if (holder != null) {
          bitmaps.add(holder.bitmap());
          context.andBaseBitmap(holder.bitmap());
        }
        if (holder == null || !holder.exact()) {
          remainings.add(child);
        }
      }
      ImmutableBitmap extracted = bitmaps.isEmpty() ? null : intersection(context.bitmapFactory(), bitmaps);
      return Pair.<ImmutableBitmap, DimFilter>of(extracted, and(remainings));
    } else {
      BitmapHolder holder = Filters.toBitmapHolder(current, context);
      if (holder != null) {
        return Pair.<ImmutableBitmap, DimFilter>of(holder.bitmap(), holder.exact() ? null : current);
      }
      return Pair.<ImmutableBitmap, DimFilter>of(null, current);
    }
  }

  // should be string type
  public static DimFilter toFilter(String dimension, List<Range> ranges)
  {
    Iterable<Range> filtered = Iterables.filter(ranges, Ranges.VALID);
    List<String> equalValues = Lists.newArrayList();
    List<DimFilter> dimFilters = Lists.newArrayList();
    for (Range range : filtered) {
      String lower = range.hasLowerBound() ? (String) range.lowerEndpoint() : null;
      String upper = range.hasUpperBound() ? (String) range.upperEndpoint() : null;
      if (lower == null && upper == null) {
        return null;
      }
      if (Objects.equals(lower, upper)) {
        equalValues.add(lower);
        continue;
      }
      boolean lowerStrict = range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN;
      boolean upperStrict = range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN;
      dimFilters.add(new BoundDimFilter(dimension, lower, upper, lowerStrict, upperStrict, false, null));
    }
    if (equalValues.size() > 1) {
      dimFilters.add(new InDimFilter(dimension, equalValues, null));
    } else if (equalValues.size() == 1) {
      dimFilters.add(new SelectorDimFilter(dimension, equalValues.get(0), null));
    }
    DimFilter filter = DimFilters.or(dimFilters).optimize(null, null);
    LOG.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, filter);
    return filter;
  }

  public static Query.FilterSupport<?> inflate(Query.FilterSupport<?> query)
  {
    final DimFilter filter = query.getFilter();
    if (filter != null) {
      List<VirtualColumn> inflated = Lists.newArrayList();
      DimFilter rewritten = Expressions.rewrite(filter, FACTORY, expression -> {
        if (expression instanceof DimFilter.VCInflator) {
          VirtualColumn vc = ((DimFilter.VCInflator) expression).inflate();
          if (vc != null) {
            inflated.add(vc);
          }
        }
        return expression;
      });
      if (!inflated.isEmpty()) {
        query = query.withVirtualColumns(GuavaUtils.concat(query.getVirtualColumns(), inflated));
      }
    }
    return query;
  }

  public static ViewDataSource inflate(ViewDataSource view)
  {
    final DimFilter filter = view.getFilter();
    if (filter != null) {
      List<VirtualColumn> inflated = Lists.newArrayList();
      DimFilter rewritten = Expressions.rewrite(filter, FACTORY, expression -> {
        if (expression instanceof DimFilter.VCInflator) {
          VirtualColumn vc = ((DimFilter.VCInflator) expression).inflate();
          if (vc != null) {
            inflated.add(vc);
          }
        }
        return expression;
      });
      if (!inflated.isEmpty()) {
        view = view.withVirtualColumns(GuavaUtils.concat(view.getVirtualColumns(), inflated));
      }
    }
    return view;
  }

  // called for non-historical nodes (see QueryResource.prepareQuery)
  public static DimFilter rewrite(DimFilter filter, Expressions.Rewriter<DimFilter> visitor)
  {
    return Expressions.rewrite(filter, FACTORY, visitor);
  }

  public static Query rewrite(Query query, Expressions.Rewriter<DimFilter> visitor)
  {
    final DimFilter filter = BaseQuery.getDimFilter(query);
    if (filter != null) {
      DimFilter rewritten = rewrite(filter, visitor);
      if (filter != rewritten) {
        query = ((Query.FilterSupport) query).withFilter(rewritten);
      }
    }
    return query;
  }

  public static ViewDataSource rewrite(ViewDataSource view, Expressions.Rewriter<DimFilter> visitor)
  {
    final DimFilter filter = view.getFilter();
    if (filter != null) {
      DimFilter rewritten = rewrite(filter, visitor);
      if (filter != rewritten) {
        view = view.withFilter(rewritten);
      }
    }
    return view;
  }

  public static final Expressions.Rewriter<DimFilter> LOG_PROVIDER = new Expressions.Rewriter<DimFilter>()
  {
    @Override
    public DimFilter visit(DimFilter expression)
    {
      if (expression instanceof DimFilter.LogProvider) {
        expression = ((DimFilter.LogProvider) expression).forLog();
      }
      return expression;
    }
  };

  public static Expressions.Rewriter<DimFilter> rewriter(final QuerySegmentWalker walker, final Query query)
  {
    return new Expressions.Rewriter<DimFilter>()
    {
      @Override
      public DimFilter visit(DimFilter expression)
      {
        if (expression instanceof DimFilter.Rewriting) {
          expression = ((DimFilter.Rewriting) expression).rewrite(walker, query);
        }
        return expression;
      }
    };
  }

  public static Expressions.Rewriter<DimFilter> decompressor(final Query query)
  {
    return filter -> filter instanceof Compressed ? ((Compressed) filter).decompress(query) : filter;
  }

  public static Expressions.Rewriter<DimFilter> compressor(final Query query)
  {
    return filter -> filter instanceof Compressible ? ((Compressible) filter).compress(query) : filter;
  }

  public static boolean hasAnyLucene(final DimFilter filter)
  {
    return filter != null && hasAny(filter, new Predicate<DimFilter>()
    {
      @Override
      public boolean apply(@Nullable DimFilter input)
      {
        return input instanceof DimFilter.LuceneFilter;
      }
    });
  }

  public static boolean hasAny(final DimFilter filter, final Predicate<DimFilter> predicate)
  {
    return Expressions.traverse(
        filter, new Expressions.Visitor<DimFilter, Boolean>()
        {
          private boolean hasAny;

          @Override
          public boolean visit(DimFilter expression)
          {
            return hasAny = predicate.apply(expression);
          }

          @Override
          public Boolean get()
          {
            return hasAny;
          }
        }
    );
  }

  public static DimFilter NONE = new None();

  public static class None implements DimFilter
  {
    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(new byte[]{0x7f, 0x00});
    }

    @Override
    public DimFilter withRedirection(Map<String, String> mapping)
    {
      return this;
    }

    @Override
    public void addDependent(Set<String> handler) {}

    @Override
    public Filter toFilter(TypeResolver resolver)
    {
      return Filters.NONE;
    }

    @Override
    public boolean equals(Object other)
    {
      return other instanceof None;
    }
  }

  public static DimFilter ALL = new All();

  public static class All implements DimFilter
  {
    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(new byte[]{0x7f, 0x01});
    }

    @Override
    public void addDependent(Set<String> handler) {}

    @Override
    public Filter toFilter(TypeResolver resolver)
    {
      return new Filter()
      {
        @Override
        public BitmapHolder getBitmapIndex(FilterContext context)
        {
          return BitmapHolder.exact(makeTrue(context.bitmapFactory(), context.numRows()));
        }

        @Override
        public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory factory)
        {
          return ValueMatcher.TRUE;
        }
      };
    }

    @Override
    public boolean equals(Object other)
    {
      return other instanceof All;
    }
  }

  public static ImmutableBitmap makeTrue(BitmapFactory factory, int numRows)
  {
    return DimFilters.complement(factory, factory.makeEmptyImmutableBitmap(), numRows);
  }

  public static ImmutableBitmap makeFalse(BitmapFactory factory)
  {
    return factory.makeEmptyImmutableBitmap();
  }

  // assumes sorted and return -1 instead of NoSuchElementException
  public static ImmutableBitmap make(BitmapFactory factory, IntIterator iterator)
  {
    if (factory instanceof RoaringBitmapFactory) {
      return RoaringBitmapFactory.copyToBitmap(iterator);
    }
    MutableBitmap mutable = factory.makeEmptyMutableBitmap();
    while (iterator.hasNext()) {
      mutable.add(iterator.next());
    }
    return factory.makeImmutableBitmap(mutable);
  }

  public static ImmutableBitmap union(BitmapFactory factory, ImmutableBitmap... bitmaps)
  {
    return factory.union(Arrays.asList(bitmaps));
  }

  public static ImmutableBitmap union(BitmapFactory factory, Iterable<ImmutableBitmap> bitmaps)
  {
    return factory.union(bitmaps);
  }

  public static ImmutableBitmap intersection(BitmapFactory factory, ImmutableBitmap... bitmaps)
  {
    return factory.intersection(Arrays.asList(bitmaps));
  }

  public static ImmutableBitmap intersection(BitmapFactory factory, Iterable<ImmutableBitmap> bitmaps)
  {
    return factory.intersection(bitmaps);
  }

  public static ImmutableBitmap complement(BitmapFactory factory, ImmutableBitmap bitmap, int length)
  {
    return factory.complement(bitmap, length);
  }

  public static ImmutableBitmap difference(
      BitmapFactory factory,
      ImmutableBitmap bitmap1,
      ImmutableBitmap bitmap2,
      int length
  )
  {
    if (bitmap1.isEmpty() || bitmap2.isEmpty()) {
      return bitmap1;
    } else if (factory instanceof RoaringBitmapFactory) {
      return ((RoaringBitmapFactory) factory).difference(bitmap1, bitmap2, length);
    } else {
      return bitmap1.difference(bitmap2);
    }
  }

  private static int lastOf(ImmutableBitmap bitmap, int limit)
  {
    if (bitmap.isEmpty()) {
      return -1;
    }
    if (bitmap instanceof WrappedImmutableRoaringBitmap) {
      return ((WrappedImmutableRoaringBitmap) bitmap).getBitmap().getReverseIntIterator().next();
    } else if (bitmap instanceof WrappedBitSetBitmap) {
      return ((WrappedBitSetBitmap) bitmap).bitset().previousSetBit(limit);
    } else if (bitmap instanceof WrappedConciseBitmap) {
//      return ((WrappedConciseBitmap) bitmap).getBitmap().last();
    }
    return limit;
  }
}
