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
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.collections.IntList;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Ranges;
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
import io.druid.query.filter.DimFilter.Discardable;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.VirtualColumn;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.Filters;
import io.druid.segment.filter.MatcherContext;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.druid.query.filter.DimFilterCacheKey.OTHER_PREFIX;

/**
 */
public class DimFilters
{
  private static final Logger LOG = new Logger(DimFilters.class);

  private static final Expression.Factory<DimFilter> FACTORY = new Expression.Factory<DimFilter>()
  {
    @Override
    public DimFilter or(List<DimFilter> children)
    {
      return DimFilters.or(children);
    }

    @Override
    public DimFilter and(List<DimFilter> children)
    {
      return DimFilters.and(children);
    }

    @Override
    public DimFilter not(DimFilter expression)
    {
      return new NotDimFilter(expression);
    }
  };

  public static DimFilter isNull(String dimension)
  {
    return new IsNullDimFilter(dimension);
  }

  private static List<DimFilter> merge(List<DimFilter> fields, DimFilter.OP op)
  {
    final IntList indices = new IntList();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i) instanceof DimFilter.Mergeable) {
        indices.add(i);
      }
    }
    if (indices.isEmpty()) {
      return fields;
    }
    boolean anyMerge = false;
    final List<DimFilter> remains = Lists.newArrayList(fields);
next:
    for (int index : indices) {
      DimFilter source = remains.get(index);
      if (!(source instanceof DimFilter.Mergeable)) {
        continue;   // merged already
      }
      DimFilter.Mergeable mergeable = (DimFilter.Mergeable) source;
      for (int i = 0; i < remains.size(); i++) {
        DimFilter filter = remains.get(i);
        if (filter == null || i == index) {
          continue;
        }
        if (mergeable.supports(op, filter)) {
          anyMerge = true;
          remains.set(i, null);
          DimFilter merged = mergeable.merge(op, filter);
          if (op == DimFilter.OP.OR && ALL.equals(merged)) {
            return Arrays.asList(ALL);
          }
          if (op == DimFilter.OP.AND && NONE.equals(merged)) {
            return Arrays.asList(NONE);
          }
          if (merged instanceof DimFilter.Mergeable) {
            mergeable = (DimFilter.Mergeable) merged;
          } else {
            remains.set(index, merged);
            continue next;
          }
        }
      }
      remains.set(index, mergeable);
    }
    return anyMerge ? Lists.newArrayList(Iterables.filter(remains, Predicates.notNull())) : fields;
  }

  public static DimFilter and(DimFilter... filters)
  {
    return and(Arrays.asList(filters));
  }

  public static final Comparator<DimFilter> ORDERING = Comparators.explicit(
      v -> v.getClass(), SelectorDimFilter.class, InDimFilter.class, BoundDimFilter.class
  );

  public static DimFilter and(List<DimFilter> filters)
  {
    if (filters.contains(NONE)) {
      return NONE;
    }
    if (filters.size() == 1) {
      return filters.get(0);
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
    return query.withFilter(
        and(DimFilters.rewrite(query.getFilter(), f -> discard(f, filter) ? null : f), filter)
    );
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
    if (filters.size() == 1) {
      return filters.get(0);
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

  public static FilterContext extractBitmaps(DimFilter filter, FilterContext context)
  {
    if (filter == null) {
      return context;
    }
    final long start = System.currentTimeMillis();
    if (filter instanceof AndDimFilter) {
      List<DimFilter> remainings = Lists.newArrayList();
      for (DimFilter child : ((AndDimFilter) filter).getChildren()) {
        BitmapHolder holder = Filters.toBitmapHolder(child, context.root(child));
        if (holder != null) {
          context.andBaseBitmap(holder.bitmap());   // for incremental access
        }
        if (holder == null || !holder.exact()) {
          remainings.add(child);
        }
      }
      context.matcher(and(remainings));
    } else {
      BitmapHolder holder = Filters.toBitmapHolder(filter, context.root(filter));
      if (holder != null) {
        context.andBaseBitmap(holder.bitmap());
      }
      if (holder == null || !holder.exact()) {
        context.matcher(filter);
      }
    }
    final ImmutableBitmap bitmap = context.baseBitmap();
    if (bitmap != null && LOG.isDebugEnabled()) {
      LOG.debug("%,d / %,d (%d msec)", bitmap.size(), context.numRows(), System.currentTimeMillis() - start);
    }
    return context;
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
      dimFilters.add(InDimFilter.of(dimension, equalValues));
    } else if (equalValues.size() == 1) {
      dimFilters.add(SelectorDimFilter.of(dimension, equalValues.get(0)));
    }
    DimFilter filter = DimFilters.or(dimFilters).optimize();
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
    return filter == null ? null : Expressions.rewrite(filter, FACTORY, visitor);
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
        expression = expression.optimize();
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

  public static boolean discard(DimFilter target, DimFilter other)
  {
    return target instanceof Discardable && ((Discardable) target).discard(other);
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
      return builder.append(new byte[]{OTHER_PREFIX, 0x00});
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

    @Override
    public String toString()
    {
      return "NONE";
    }
  }

  public static DimFilter ALL = new All();

  public static class All implements DimFilter
  {
    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.append(new byte[]{OTHER_PREFIX, 0x01});
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

    @Override
    public String toString()
    {
      return "ALL";
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

  // it's not serious
  public static double cost(DimFilter filter)
  {
    if (filter == null) {
      return 0;
    } else if (filter instanceof Expression.RelationExpression) {
      return ((Expression.RelationExpression) filter).getChildren().stream().mapToDouble(f -> cost((DimFilter) f)).sum();
    }
    if (filter instanceof SelectorDimFilter || filter instanceof IsNullDimFilter) {
      return 0.001;
    } else if (filter instanceof InDimFilter) {
      return Math.min(0.1, 0.001 * ((InDimFilter) filter).getValues().size());
    } else if (filter instanceof PrefixDimFilter) {
      return 0.01;
    } else if (filter instanceof BoundDimFilter || filter instanceof LikeDimFilter) {
      return 0.04;
    } else if (filter instanceof MathExprFilter || filter instanceof RegexDimFilter) {
      return 0.1;
    } else if (filter instanceof BloomDimFilter || filter instanceof InDimsFilter) {
      return 0.2;
    } else if (filter instanceof Compressed || filter instanceof DimFilter.LuceneFilter) {
      return 0.4;
    }
    return 0.5;
  }
}
