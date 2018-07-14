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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitSetBitmapFactory;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.collections.bitmap.WrappedImmutableRoaringBitmap;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.cache.Cache;
import io.druid.common.guava.IntPredicate;
import io.druid.common.utils.Ranges;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Expression;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.math.expr.Expression.FuncExpression;
import io.druid.math.expr.Expression.NotExpression;
import io.druid.math.expr.Expression.OrExpression;
import io.druid.math.expr.Expressions;
import io.druid.math.expr.Parser;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BitmapType;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.Filter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.SecondaryIndex;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.lucene.Lucenes;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 */
public class Filters
{
  private static final Logger logger = new Logger(Filters.class);

  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   *
   * @return list of Filters
   */
  public static List<Filter> toFilters(List<DimFilter> dimFilters)
  {
    return ImmutableList.copyOf(
        FunctionalIterable
            .create(dimFilters)
            .transform(
                new Function<DimFilter, Filter>()
                {
                  @Override
                  public Filter apply(DimFilter input)
                  {
                    return input.toFilter();
                  }
                }
            )
    );
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  public static Filter toFilter(DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.optimize().toFilter();
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(ColumnSelectorFactory factory, String column, Predicate predicate)
  {
    ValueDesc columnType = factory.getColumnType(column);
    if (columnType == null) {
      return BooleanValueMatcher.of(predicate.apply(null));
    }
    if (ValueDesc.isDimension(columnType)) {
      return Filters.toValueMatcher(factory.makeDimensionSelector(DefaultDimensionSpec.of(column)), predicate);
    }
    return Filters.toValueMatcher(ColumnSelectors.toDimensionalSelector(factory, column), predicate);
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(
      final ObjectColumnSelector selector,
      final Predicate predicate
  )
  {
    final ValueDesc type = selector.type();
    if (ValueDesc.isPrimitive(type)) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          return predicate.apply(selector.get());
        }
      };
    }
    if (ValueDesc.isArray(type) || ValueDesc.isMultiValued(type)) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final Object object = selector.get();
          if (object == null || !object.getClass().isArray()) {
            return predicate.apply(object);
          }
          final int length = Array.getLength(object);
          for (int i = 0; i < length; i++) {
            if (predicate.apply(Array.get(object, i))) {
              return true;
            }
          }
          return false;
        }
      };
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.apply(selector.get());
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final Predicate predicate
  )
  {
    final boolean allowNull = predicate.apply(null);
    // Check every value in the dimension, as a String.
    final int cardinality = selector.getValueCardinality();
    if (cardinality < 0) {
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int length = row.size();
          if (length == 0) {
            return allowNull;
          } else if (length == 1) {
            return predicate.apply(selector.lookupName(row.get(0)));
          }
          for (int i = 0; i < length; i++) {
            if (predicate.apply(selector.lookupName(row.get(i)))) {
              return true;
            }
          }
          return false;
        }
      };
    }

    final BitSet valueIds = new BitSet(cardinality);
    for (int i = 0; i < cardinality; i++) {
      if (predicate.apply(selector.lookupName(i))) {
        valueIds.set(i);
      }
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int length = row.size();
        if (length == 0) {
          return allowNull;
        } else if (length == 1) {
          return valueIds.get(row.get(0));
        }
        for (int i = 0; i < length; i++) {
          if (valueIds.get(row.get(i))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static ValueMatcher toValueMatcher(
      final DimensionSelector selector,
      final IntPredicate predicate,
      final boolean allowNull
  )
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        final IndexedInts row = selector.getRow();
        final int length = row.size();
        if (length == 0) {
          return allowNull;
        }
        for (int i = 0; i < length; i++) {
          if (predicate.apply(row.get(i))) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static ImmutableBitmap matchPredicateValues(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      return selector.getBitmapFactory().makeEmptyImmutableBitmap();
    }

    final BitmapFactory factory = selector.getBitmapFactory();
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (int i = 0; i < dimValues.size(); i++) {
      if (predicate.apply(dimValues.get(i))) {
        bitmap.add(i);
      }
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  /**
   * Return the union of bitmaps for all values matching a particular predicate.
   *
   * @param dimension dimension to look at
   * @param selector  bitmap selector
   * @param predicate predicate to use
   *
   * @return bitmap of matching rows
   */
  public static ImmutableBitmap matchPredicate(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      if (predicate.apply(null)) {
        return DimFilters.makeTrue(selector.getBitmapFactory(), selector.getNumRows());
      } else {
        return DimFilters.makeFalse(selector.getBitmapFactory());
      }
    }

    // Apply predicate to all dimension values and union the matching bitmaps
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    return selector.getBitmapFactory().union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = 0;

              @Override
              public boolean hasNext()
              {
                return currIndex < bitmapIndex.getCardinality();
              }

              @Override
              public ImmutableBitmap next()
              {
                while (currIndex < bitmapIndex.getCardinality() &&
                       !predicate.apply(dimValues.get(currIndex))) {
                  currIndex++;
                }

                if (currIndex == bitmapIndex.getCardinality()) {
                  return bitmapIndex.getBitmapFactory().makeEmptyImmutableBitmap();
                }

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

  public static DimFilter[] partitionWithBitmapSupport(DimFilter current, final RowResolver resolver)
  {
    current = Filters.convertToCNF(current);
    if (current == null) {
      return null;
    }
    return partitionFilterWith(
        current, new Predicate<DimFilter>()
        {
          @Override
          public boolean apply(DimFilter input)
          {
            return resolver.supportsBitmap(input, BitmapType.EXACT);
          }
        }
    );
  }

  public static Set<String> getDependents(DimFilter filter)
  {
    Set<String> handler = Sets.newHashSet();
    if (filter != null) {
      filter.addDependent(handler);
    }
    return handler;
  }

  public static class BitmapHolder extends Pair<Boolean, ImmutableBitmap>
  {
    public static BitmapHolder of(boolean exact, ImmutableBitmap rhs)
    {
      return new BitmapHolder(exact, rhs);
    }

    public static BitmapHolder exact(ImmutableBitmap rhs)
    {
      return new BitmapHolder(true, rhs);
    }

    public static BitmapHolder notExact(ImmutableBitmap rhs)
    {
      return new BitmapHolder(false, rhs);
    }

    private BitmapHolder(boolean lhs, ImmutableBitmap rhs)
    {
      super(lhs, rhs);
    }

    public boolean exact()
    {
      return lhs;
    }

    public ImmutableBitmap bitmap()
    {
      return rhs;
    }
  }

  private static final BitmapFactory[] BITMAP_FACTORIES = new BitmapFactory[]{
      new BitSetBitmapFactory(), new ConciseBitmapFactory(), new RoaringBitmapFactory()
  };

  public static FilterContext getFilterContext(
      final BitmapIndexSelector selector,
      final Cache cache,
      final String segmentId
  )
  {
    final byte segmentCode = codeOfFactory(selector.getBitmapFactory());
    if (cache == null || segmentId == null || segmentCode >= BITMAP_FACTORIES.length) {
      return new FilterContext(selector);
    }
    return new FilterContext(selector)
    {
      @Override
      public BitmapHolder createBitmap(DimFilter filter, EnumSet<BitmapType> include)
      {
        Cache.NamedKey key = new Cache.NamedKey(segmentId, filter.getCacheKey());
        byte[] cached = cache.get(key);
        if (cached != null) {
          ByteBuffer wrapped = ByteBuffer.wrap(cached);
          byte code = wrapped.get();
          boolean exact = wrapped.get() != 0;
          if (code == segmentCode) {
            return BitmapHolder.of(exact, factory.mapImmutableBitmap(wrapped));
          }
          MutableBitmap mutable = factory.makeEmptyMutableBitmap();
          IntIterator iterators = BITMAP_FACTORIES[code].mapImmutableBitmap(wrapped).iterator();
          while (iterators.hasNext()) {
            mutable.add(iterators.next());
          }
          return BitmapHolder.of(exact, factory.makeImmutableBitmap(mutable));
        }
        BitmapHolder holder = super.createBitmap(filter, include);
        if (holder != null) {
          byte exact = holder.exact() ? (byte) 0x01 : 0x00;
          cache.put(key, StringUtils.concat(new byte[]{segmentCode, exact}, holder.bitmap().toBytes()));
        }
        return holder;
      }
    };
  }

  private static byte codeOfFactory(BitmapFactory factory)
  {
    if (factory instanceof BitSetBitmapFactory) {
      return 0x00;
    } else if (factory instanceof ConciseBitmapFactory) {
      return 0x01;
    } else if (factory instanceof RoaringBitmapFactory) {
      return 0x02;
    }
    return 0x7f;
  }

  public static class FilterContext implements Closeable
  {
    protected final BitmapIndexSelector selector;
    protected final BitmapFactory factory;
    protected ImmutableBitmap baseBitmap;

    public FilterContext(BitmapIndexSelector selector)
    {
      this.selector = selector;
      this.factory = selector.getBitmapFactory();
    }

    public BitmapHolder createBitmap(DimFilter filter, EnumSet<BitmapType> include)
    {
      return _toBitmapHolder(filter, this, include);
    }

    public BitmapFactory bitmapFactory()
    {
      return factory;
    }

    public ImmutableBitmap intersection(List<ImmutableBitmap> bitmaps)
    {
      return factory.intersection(bitmaps);
    }

    public ImmutableBitmap union(List<ImmutableBitmap> bitmaps)
    {
      return factory.union(bitmaps);
    }

    public void setBaseBitmap(ImmutableBitmap baseBitmap)
    {
      this.baseBitmap = baseBitmap;
    }

    public int getNumRows()
    {
      return selector.getNumRows();
    }

    @Override
    public void close()
    {
      selector.close();
    }
  }

  public static ImmutableBitmap toBitmap(
      DimFilter dimFilter,
      FilterContext context,
      EnumSet<BitmapType> include
  )
  {
    BitmapHolder holder = toBitmapHolder(dimFilter, context, include);
    return holder == null ? null : holder.bitmap();
  }

  public static BitmapHolder toBitmapHolder(
      DimFilter dimFilter,
      FilterContext context,
      EnumSet<BitmapType> include
  )
  {
    long start = System.currentTimeMillis();
    BitmapHolder baseBitmap = null;
    if (dimFilter instanceof Expression.AndExpression) {
      boolean exact = true;
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((Expression.AndExpression) dimFilter).getChildren()) {
        DimFilter filter = (DimFilter) child;
        BitmapHolder bitmap = context.createBitmap(filter, include);
        if (bitmap != null) {
          logger.debug("%s : %,d / %,d", filter, bitmap.rhs.size(), context.getNumRows());
          bitmaps.add(bitmap.rhs);
          exact &= bitmap.lhs;
        } else {
          exact = false;
        }
      }
      if (!bitmaps.isEmpty()) {
        // concise returns 1,040,187,360. roaring returns 0. makes wrong result anyway
        baseBitmap = new BitmapHolder(exact, context.intersection(bitmaps));
      }
    } else {
      baseBitmap = context.createBitmap(dimFilter, include);
    }

    if (baseBitmap != null) {
      long elapsed = System.currentTimeMillis() - start;
      logger.debug("%s : %,d / %,d (%,d msec)", dimFilter, baseBitmap.rhs.size(), context.getNumRows(), elapsed);
    }
    return baseBitmap;
  }

  private static BitmapHolder _toBitmapHolder(
      DimFilter dimFilter,
      FilterContext context,
      EnumSet<BitmapType> using
  )
  {
    BitmapIndexSelector selector = context.selector;
    Set<String> dependents = Filters.getDependents(dimFilter);
    if (dependents.size() == 1 && using.contains(BitmapType.DIMENSIONAL)) {
      String dimension = Iterables.getOnlyElement(dependents);
      ColumnCapabilities capabilities = selector.getCapabilities(dimension);
      if (capabilities != null && capabilities.hasBitmapIndexes()) {
        if (context.baseBitmap == null ||
            selector.getBitmapIndex(dimension).getCardinality() < context.baseBitmap.size()) {
          return BitmapHolder.exact(dimFilter.toFilter().getBitmapIndex(selector, using, context.baseBitmap));
        }
      }
    }
    BitmapHolder bitmap = toExternalBitmap(dimFilter, context, using);
    if (bitmap == null && using.equals(BitmapType.EXACT)) {
      throw new UnsupportedOperationException("cannot make exact bitmap from " + dimFilter);
    }
    return bitmap;
  }

  @SuppressWarnings("unchecked")
  private static BitmapHolder toExternalBitmap(
      DimFilter filter,
      FilterContext context,
      EnumSet<BitmapType> using
  )
  {
    // todo move this into filter itself (see SelectorFilter)
    BitmapIndexSelector selector = context.selector;
    if (filter instanceof InDimFilter) {
      InDimFilter inFilter = (InDimFilter) filter;
      SecondaryIndex metricBitmap = getWhatever(selector, inFilter.getDimension(), using);
      if (metricBitmap instanceof HistogramBitmap || metricBitmap instanceof BitSlicedBitmap) {
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (String value : inFilter.getValues()) {
          ImmutableBitmap bitmap = metricBitmap.filterFor(Range.closed(value, value));
          if (bitmap == null) {
            return null;
          }
          bitmaps.add(bitmap);
        }
        return bitmaps.isEmpty() ? null : BitmapHolder.notExact(selector.getBitmapFactory().union(bitmaps));
      } else if (metricBitmap instanceof LuceneIndex) {
        List<Term> terms = Lists.newArrayList();
        for (String value : inFilter.getValues()) {
          terms.add(new Term(inFilter.getDimension(), value));
        }
        return BitmapHolder.exact(metricBitmap.filterFor(new TermsQuery(terms)));
      }
    } else if (filter instanceof MathExprFilter) {
      Expr expr = Parser.parse(((MathExprFilter) filter).getExpression());
      Expr cnf = Expressions.convertToCNF(expr, Parser.EXPR_FACTORY);
      ImmutableBitmap bitmap = toExprBitmap(cnf, selector, using, false);
      return bitmap == null ? null : BitmapHolder.exact(bitmap);
    } else if (filter instanceof DimFilter.LuceneFilter) {
      return BitmapHolder.exact(filter.toFilter().getBitmapIndex(selector, using, context.baseBitmap));
    }
    return null;
  }

  private static ImmutableBitmap toExprBitmap(
      Expression tree,
      BitmapIndexSelector bitmapSelector,
      EnumSet<BitmapType> using,
      boolean withNot
  )
  {
    BitmapFactory factory = bitmapSelector.getBitmapFactory();
    if (tree instanceof FuncExpression) {
      List<String> required = Parser.findRequiredBindings((Expr) tree);
      if (required.size() != 1) {
        return null;
      }
      String column = required.get(0);
      SecondaryIndex metric = getWhatever(bitmapSelector, column, using);
      if (metric == null) {
        return null;
      }
      ImmutableBitmap bitmap = leafToRanges(column, (FuncExpression) tree, factory, metric, withNot);
      if (bitmap != null) {
        logger.debug("%s%s : %,d / %,d", withNot ? "!" : "", tree, bitmap.size(), metric.rows());
      }
      return bitmap;
    } else if (tree instanceof AndExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((Expression.BooleanExpression) tree).getChildren()) {
        ImmutableBitmap extracted = toExprBitmap(child, bitmapSelector, using, withNot);
        if (extracted != null) {
          bitmaps.add(extracted);
        }
      }
      return bitmaps.isEmpty() ? null : factory.intersection(bitmaps);
    } else if (tree instanceof OrExpression) {
      List<ImmutableBitmap> bitmaps = Lists.newArrayList();
      for (Expression child : ((Expression.BooleanExpression) tree).getChildren()) {
        ImmutableBitmap extracted = toExprBitmap(child, bitmapSelector, using, withNot);
        if (extracted == null) {
          return null;
        }
        bitmaps.add(extracted);
      }
      return bitmaps.isEmpty() ? null : factory.union(bitmaps);
    } else if (tree instanceof NotExpression) {
      return toExprBitmap(((NotExpression) tree).getChild(), bitmapSelector, using, !withNot);
    }
    return null;
  }

  private static SecondaryIndex getWhatever(BitmapIndexSelector bitmaps, String column, EnumSet<BitmapType> using)
  {
    SecondaryIndex bitmap = null;
    if (using.contains(BitmapType.HISTOGRAM_BITMAP)) {
      bitmap = bitmaps.getMetricBitmap(column);
    }
    if (bitmap == null && using.contains(BitmapType.BSB)) {
      bitmap = bitmaps.getBitSlicedBitmap(column);
    }
    if (bitmap == null && using.contains(BitmapType.LUCENE_INDEX)) {
      bitmap = bitmaps.getLuceneIndex(column);
    }
    return bitmap;
  }

  // constants need to be calculated apriori
  @SuppressWarnings("unchecked")
  private static ImmutableBitmap leafToRanges(
      String column,
      FuncExpression expression,
      BitmapFactory factory,
      SecondaryIndex metric,
      boolean withNot
  )
  {
    final ValueType type = ValueDesc.assertPrimitive(metric.type()).type();

    if (Expressions.isCompare(expression.op())) {
      final Comparable constant = getOnlyConstant(expression.getChildren(), type);
      if (constant == null) {
        return null;
      }
      switch (expression.op()) {
        case "<":
          return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ? metric.filterFor(
                     withNot ? Range.atLeast(constant) : Range.lessThan(constant)
                 ) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atLeast(column, constant) : Lucenes.lessThan(column, constant)
                 ) : null;
        case ">":
          return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ? metric.filterFor(
                     withNot ? Range.atMost(constant) : Range.greaterThan(constant)
                 ) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.atMost(column, constant) : Lucenes.greaterThan(column, constant)
                 ) : null;
        case "<=":
          return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ? metric.filterFor(
                     withNot ? Range.greaterThan(constant) : Range.atMost(constant)
                 ) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.greaterThan(column, constant) : Lucenes.atMost(column, constant)
                 ) : null;
        case ">=":
          return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ? metric.filterFor(
                     withNot ? Range.lessThan(constant) : Range.atLeast(constant)
                 ) :
                 metric instanceof LuceneIndex ? metric.filterFor(
                     withNot ? Lucenes.lessThan(column, constant) : Lucenes.atLeast(column, constant)
                 ) : null;
        case "==":
          if (withNot) {
            return factory.union(
                Arrays.asList(
                    metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
                    metric.filterFor(Range.lessThan(constant)) :
                    metric.filterFor(Lucenes.lessThan(column, constant)),
                    metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
                    metric.filterFor(Range.greaterThan(constant)) :
                    metric.filterFor(Lucenes.greaterThan(column, constant))
                )
            );
          }
          return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
                 metric.filterFor(Range.closed(constant, constant)) :
                 metric.filterFor(Lucenes.point(column, constant));
      }
    }

    switch (expression.op()) {
      case "between":
        List<Comparable> constants = getConstants(expression.getChildren(), type);
        if (constants.size() != 2) {
          return null;
        }
        Comparable value1 = constants.get(0);
        Comparable value2 = constants.get(1);
        if (value1 == null || value2 == null) {
          return null;
        }
        if (value1.compareTo(value2) > 0) {
          Comparable x = value1;
          value1 = value2;
          value2 = x;
        }
        if (withNot) {
          return factory.union(
              Arrays.asList(
                  metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
                  metric.filterFor(Range.lessThan(value1)) :
                  metric.filterFor(Lucenes.lessThan(column, value1)),
                  metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
                  metric.filterFor(Range.greaterThan(value2)) :
                  metric.filterFor(Lucenes.greaterThan(column, value2))
              )
          );
        }
        return metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
               metric.filterFor(Range.closed(value1, value2)) :
               metric.filterFor(Lucenes.closed(column, value1, value2));
      case "in":
        if (withNot) {
          return null;  // hard to be expressed with bitmap
        }
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (Comparable value : getConstants(expression.getChildren(), type)) {
          bitmaps.add(
              metric instanceof HistogramBitmap || metric instanceof BitSlicedBitmap ?
              metric.filterFor(Range.closed(value, value)) :
              metric.filterFor(Lucenes.point(column, value))
          );
        }
        return bitmaps.isEmpty() ? null : factory.union(bitmaps);
    }
    return null;
  }

  private static Comparable getOnlyConstant(List<Expression> children, ValueType type)
  {
    Comparable constant = null;
    for (Expression expr : children) {
      if (expr instanceof Expression.ConstExpression) {
        if (constant != null) {
          return null;
        }
        constant = type.cast(((Expression.ConstExpression) expr).get());
      }
    }
    return constant;
  }

  private static List<Comparable> getConstants(List<Expression> children, ValueType type)
  {
    List<Comparable> constants = Lists.newArrayList();
    for (Expression expr : children) {
      if (expr instanceof Expression.ConstExpression) {
        constants.add(type.cast(((Expression.ConstExpression) expr).get()));
      }
    }
    return constants;
  }

  private static DimFilter[] partitionFilterWith(DimFilter current, Predicate<DimFilter> predicate)
  {
    if (current == null) {
      return null;
    }
    List<DimFilter> bitmapIndexSupported = Lists.newArrayList();
    List<DimFilter> bitmapIndexNotSupported = Lists.newArrayList();

    traverse(current, predicate, bitmapIndexSupported, bitmapIndexNotSupported);

    return new DimFilter[]{andFilter(bitmapIndexSupported), andFilter(bitmapIndexNotSupported)};
  }

  private static void traverse(
      DimFilter current,
      Predicate<DimFilter> predicate,
      List<DimFilter> support,
      List<DimFilter> notSupport
  )
  {
    if (current instanceof AndDimFilter) {
      for (DimFilter child : ((AndDimFilter) current).getChildren()) {
        traverse(child, predicate, support, notSupport);
      }
    } else {
      if (predicate.apply(current)) {
        support.add(current);
      } else {
        notSupport.add(current);
      }
    }
  }

  private static DimFilter andFilter(List<DimFilter> filters)
  {
    return filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0) : new AndDimFilter(filters);
  }

  public static DimFilter convertToCNF(DimFilter current)
  {
    return current == null ? null : Expressions.convertToCNF(current.optimize(), DimFilter.FACTORY);
  }

  public static Filter convertToCNF(Filter current)
  {
    return Expressions.convertToCNF(current, new Filter.Factory());
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
    DimFilter dimFilter = new OrDimFilter(dimFilters).optimize();
    logger.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, dimFilter);
    return dimFilter;
  }

  public static ImmutableBitmap intersection(
      BitmapFactory bitmapFactory,
      @Nullable ImmutableBitmap baseBitmap,
      @Nullable ImmutableBitmap bitmap
  )
  {
    if (baseBitmap == null) {
      return bitmap;
    }
    if (bitmap == null) {
      return baseBitmap;
    }
    return bitmapFactory.intersection(Arrays.asList(baseBitmap, bitmap));
  }

  public static boolean hasBitmapOrNull(BitmapIndexSelector selector, String dimension)
  {
    ColumnCapabilities capabilities = selector.getCapabilities(dimension);
    return capabilities == null || capabilities.hasBitmapIndexes();
  }

  public static IntPredicate toMatcher(ImmutableBitmap bitmapIndex, boolean descending)
  {
    final IntIterator iterator = newIterator(bitmapIndex, descending);
    if (!iterator.hasNext()) {
      return new IntPredicate()
      {
        @Override
        public boolean apply(int value)
        {
          return false;
        }
      };
    }
    if (!descending) {
      return new IntPredicate()
      {
        private int peek = iterator.next();

        @Override
        public boolean apply(int value)
        {
          while (peek >= 0) {
            if (peek == value) {
              peek = iterator.hasNext() ? iterator.next() : -1;
              return true;
            } else if (peek > value) {
              return false;
            } else {
              peek = iterator.hasNext() ? iterator.next() : -1;
            }
          }
          return false;
        }
      };
    } else {
      return new IntPredicate()
      {
        private int peek = iterator.next();

        @Override
        public boolean apply(int value)
        {
          while (peek >= 0) {
            if (peek == value) {
              peek = iterator.hasNext() ? iterator.next() : -1;
              return true;
            } else if (peek < value) {
              return false;
            } else {
              peek = iterator.hasNext() ? iterator.next() : -1;
            }
          }
          return false;
        }
      };
    }
  }

  public static IntIterator newIterator(ImmutableBitmap bitmapIndex, boolean descending)
  {
    if (!descending) {
      return bitmapIndex.iterator();
    }
    ImmutableBitmap roaringBitmap = bitmapIndex;
    if (!(bitmapIndex instanceof WrappedImmutableRoaringBitmap)) {
      final BitmapFactory factory = RoaringBitmapSerdeFactory.bitmapFactory;
      final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
      final IntIterator iterator = bitmapIndex.iterator();
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      roaringBitmap = factory.makeImmutableBitmap(bitmap);
    }
    return ((WrappedImmutableRoaringBitmap) roaringBitmap).getBitmap().getReverseIntIterator();
  }

  public static boolean isAllLucene(DimFilter filter)
  {
    return Expressions.traverse(
        filter, new Expressions.Visitor.Void()
        {
          @Override
          public boolean visit(Expression expression)
          {
            return expression instanceof DimFilter.LuceneFilter;
          }
        }
    );
  }
}
