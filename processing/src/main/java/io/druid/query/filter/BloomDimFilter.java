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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.collections.IntList;
import io.druid.common.KeyBuilder;
import io.druid.common.Scannable.BufferBacked;
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.DSuppliers;
import io.druid.common.utils.IOUtils;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.HashAggregator;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.aggregation.bloomfilter.BloomFilterAggregatorFactory;
import io.druid.query.aggregation.bloomfilter.BloomKFilter;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.filter.MatcherContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.druid.query.filter.DimFilter.BestEffort;
import static io.druid.query.filter.DimFilter.LogProvider;

/**
 */
@JsonTypeName("bloom")
public class BloomDimFilter implements LogProvider, BestEffort
{
  private static final float BULKSCAN_THRESHOLD_RATIO = 1.33f;

  public static BloomDimFilter of(List<String> fieldNames, BloomKFilter filter)
  {
    return new BloomDimFilter(fieldNames, null, GroupingSetSpec.EMPTY, filter.serialize());
  }

  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final GroupingSetSpec groupingSets;
  private final byte[] bloomFilter;
  private final Supplier<BloomKFilter> supplier;
  private final Supplier<HashCode> hash;

  @JsonCreator
  public BloomDimFilter(
      @JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("fields") List<DimensionSpec> fields,
      @JsonProperty("groupingSets") GroupingSetSpec groupingSets,
      @JsonProperty("bloomFilter") byte[] bloomFilter
  )
  {
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.bloomFilter = Preconditions.checkNotNull(bloomFilter);
    this.supplier = DSuppliers.memoize(() -> BloomKFilter.deserialize(bloomFilter));
    this.hash = DSuppliers.memoize(() -> Hashing.murmur3_128().hashBytes(bloomFilter));
    Preconditions.checkArgument(
        fieldNames != null ^ fields != null,
        "Must have a valid, non-null fieldNames or fields"
    );
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(DimFilterCacheKey.BLOOM_CACHE_ID)
                  .append(fieldNames)
                  .append(fields)
                  .append(groupingSets)
                  .append(bloomFilter.length)
                  .append(hash.get().asBytes());
  }

  @Override
  public void addDependent(Set<String> handler)
  {
    if (fieldNames != null) {
      handler.addAll(fieldNames);
    } else {
      handler.addAll(DimensionSpecs.toInputNames(fields));
    }
  }

  @Override
  public Filter toFilter(TypeResolver resolver)
  {
    return new Filter()
    {
      @Override
      public BitmapHolder getBitmapIndex(FilterContext context)
      {
        if (!context.isRoot(BloomDimFilter.this)) {
          return null;
        }
        // todo support multi dimension by looping ?
        final String dimension;
        if (fields != null && fields.size() == 1 && fields.get(0) instanceof DefaultDimensionSpec) {
          dimension = fields.get(0).getDimension();
        } else if (fieldNames != null && fieldNames.size() == 1) {
          dimension = fieldNames.get(0);
        } else {
          return null;
        }
        final BitmapIndexSelector selector = context.indexSelector();
        final Column column = selector.getColumn(dimension);
        if (column == null) {
          return null;
        }
        final BitmapFactory factory = selector.getBitmapFactory();
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex == null) {
          if (column.hasGenericColumn() && BufferBacked.class.isAssignableFrom(column.getGenericColumnType())) {
            final GenericColumn generic = column.getGenericColumn();
            try {
              final BufferBacked scannable = (BufferBacked) generic;
              final MutableBitmap mutable = factory.makeEmptyMutableBitmap();
              final BloomKFilter filter = supplier.get();
              scannable.scan(context.rowIterator(), (x, b, o, l) -> {
                if (filter.testHash(Murmur3.hash64(b, o, l))) {mutable.add(x);}
              });
              return BitmapHolder.exact(factory.makeImmutableBitmap(mutable));
            }
            finally {
              IOUtils.closePropagate(generic);
            }
          }
          return null;
        }
        final DictionaryEncodedColumn encoded = column.getDictionaryEncoding();
        try {
          final int cardinality = context.dictionaryRange(dimension, encoded.dictionary().size());
          final IntIterator iterator;
          if (encoded.hasMultipleValues() || context.notFiltered() || cardinality < context.targetNumRows()) {
            iterator = context.dictionaryIterator(dimension);
          } else {
            final IntSet set = new IntOpenHashSet();
            encoded.scan(context.rowIterator(), (x, v) -> set.add(v.applyAsInt(x)));
            iterator = IntIterators.from(IntList.of(set.toIntArray()).sort());
          }
          if (iterator != null && !iterator.hasNext()) {
            return BitmapHolder.exact(factory.makeEmptyImmutableBitmap());
          }
          final IntList matched = new IntList();
          final BloomKFilter filter = supplier.get();
          encoded.dictionary().scan(iterator, (x, b, o, l) -> {
            if (filter.testHash(Murmur3.hash64(b, o, l))) {matched.add(x);}
          });
          context.dictionaryRef(dimension, RoaringBitmapFactory.from(matched.array()));

          // actually it's not exact. but if matcher cannot improve that, it can be said it's exact
          final GenericIndexed<ImmutableBitmap> bitmaps = bitmapIndex.getBitmaps();
          return BitmapHolder.exact(factory.union(matched.transform(x -> bitmaps.get(x))));
        }
        finally {
          IOUtils.closePropagate(encoded);
        }
      }

      @Override
      public ValueMatcher makeMatcher(MatcherContext context, ColumnSelectorFactory columnFactory)
      {
        List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
        List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

        int[][] grouping = GroupingSetSpec.EMPTY_INDEX;
        if (groupingSets != null) {
          grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
        }
        final BloomTest tester = new BloomTest(supplier.get());
        final HashAggregator<BloomTest> aggregator = new BloomTestAggregator(selectors, grouping);

        return () -> aggregator.aggregate(tester).status;
      }
    };
  }

  @Override
  public DimFilter forLog()
  {
    return new BloomDimFilter(fieldNames, fields, groupingSets, StringUtils.EMPTY_BYTES);
  }

  private static class BloomTestAggregator extends HashAggregator<BloomTest>
  {
    public BloomTestAggregator(List<DimensionSelector> selectorList, int[][] groupings)
    {
      super(null, selectorList, groupings, false, false);
    }

    @Override
    protected Class<BloomTest> collectorClass()
    {
      return BloomTest.class;
    }
  }

  private static class BloomTest implements HashCollector.ScanSupport
  {
    private final BloomKFilter filter;
    private boolean status;

    private BloomTest(BloomKFilter filter) {this.filter = filter;}

    @Override
    public void collect(Object[] values, BytesRef bytes)
    {
      status = filter.test(bytes);
    }

    @Override
    public void collect(DimensionSelector.Scannable scannable)
    {
      status = filter.test(scannable);
    }
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSpec> getFields()
  {
    return fields;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
  }

  @JsonProperty
  public byte[] getBloomFilter()
  {
    return bloomFilter;
  }

  @Override
  public String toString()
  {
    if (fieldNames != null) {
      return "BloomFilter{fieldNames=" + fieldNames + ", groupingSets=" + groupingSets + '}';
    } else {
      return "BloomFilter{fields=" + fields + ", groupingSets=" + groupingSets + '}';
    }
  }

  @JsonTypeName("bloom.factory")
  public static class Factory extends FilterFactory implements Rewriting
  {
    private static final Logger LOG = new Logger(BloomDimFilter.Factory.class);

    public static BloomDimFilter.Factory fieldNames(List<String> fieldNames, ViewDataSource source, int maxNumEntries)
    {
      return new BloomDimFilter.Factory(fieldNames, null, GroupingSetSpec.EMPTY, source, maxNumEntries);
    }

    public static BloomDimFilter.Factory fields(List<DimensionSpec> fields, ViewDataSource source, int maxNumEntries)
    {
      return new BloomDimFilter.Factory(null, fields, GroupingSetSpec.EMPTY, source, maxNumEntries);
    }

    private final List<String> fieldNames;
    private final List<DimensionSpec> fields;
    private final GroupingSetSpec groupingSets;
    private final ViewDataSource bloomSource;
    private final int maxNumEntries;

    @JsonCreator
    public Factory(
        @JsonProperty("fieldNames") List<String> fieldNames,
        @JsonProperty("fields") List<DimensionSpec> fields,
        @JsonProperty("groupingSets") GroupingSetSpec groupingSets,
        @JsonProperty("bloomSource") ViewDataSource bloomSource,
        @JsonProperty("maxNumEntries") int maxNumEntries
    )
    {
      this.fieldNames = fieldNames;
      this.fields = fields;
      this.groupingSets = groupingSets;
      this.bloomSource = Preconditions.checkNotNull(bloomSource);
      this.maxNumEntries = maxNumEntries;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<String> getFieldNames()
    {
      return fieldNames;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public List<DimensionSpec> getFields()
    {
      return fields;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public GroupingSetSpec getGroupingSets()
    {
      return groupingSets;
    }

    @JsonProperty
    public ViewDataSource getBloomSource()
    {
      return bloomSource;
    }

    @JsonProperty
    public int getMaxNumEntries()
    {
      return maxNumEntries;
    }

    @Override
    public DimFilter rewrite(QuerySegmentWalker walker, Query parent)
    {
      TimeseriesQuery query = new TimeseriesQuery.Builder()
          .dataSource(bloomSource.getName())
          .filters(bloomSource.getFilter())
          .context(BaseQuery.copyContextForMeta(parent))
          .addContext(Query.USE_CACHE, false)
          .addContext(Query.POPULATE_CACHE, false)
          .build();

      int expectedCardinality = maxNumEntries;
      if (expectedCardinality <= 0) {
        query = query.withAggregatorSpecs(
            Arrays.asList(CardinalityAggregatorFactory.fields("$cardinality", bloomSource.getColumns(), groupingSets))
        );
        expectedCardinality = Ints.checkedCast(
            Sequences.only(QueryRunners.run(query, walker)).getLongMetric("$cardinality")
        );
      }
      query = query.withAggregatorSpecs(
          Arrays.asList(BloomFilterAggregatorFactory.of("$bloom", bloomSource.getColumns(), expectedCardinality))
      );
      BloomKFilter filter = (BloomKFilter) Sequences.only(QueryRunners.run(query, walker)).getRaw("$bloom");
      LOG.debug("-- bloom filter generated for [%s:%d]", BaseQuery.getAlias(parent), expectedCardinality);
      return new BloomDimFilter(fieldNames, fields, groupingSets, filter.serialize());
    }

    @Override
    public String toString()
    {
      return "BloomDimFilter.Factory{" +
             "bloomSource=" + bloomSource +
             (fieldNames == null ? "" : ", fieldNames=" + fieldNames) +
             (fields == null ? "" : ", fields=" + fields) +
             (groupingSets == null ? "" : ", groupingSets=" + groupingSets) +
             ", maxNumEntries=" + maxNumEntries +
             '}';
    }
  }

  public static class Lazy extends FilterFactory implements Rewriting
  {
    public static BloomDimFilter.Lazy fieldNames(List<String> fieldNames, Supplier<BloomKFilter> supplier)
    {
      return new BloomDimFilter.Lazy(fieldNames, null, GroupingSetSpec.EMPTY, supplier);
    }

    public static BloomDimFilter.Lazy fields(List<DimensionSpec> fields, Supplier<BloomKFilter> supplier)
    {
      return new BloomDimFilter.Lazy(null, fields, GroupingSetSpec.EMPTY, supplier);
    }

    private final List<String> fieldNames;
    private final List<DimensionSpec> fields;
    private final GroupingSetSpec groupingSets;
    private final Supplier<BloomKFilter> supplier;

    private Lazy(
        List<String> fieldNames,
        List<DimensionSpec> fields,
        GroupingSetSpec groupingSets,
        Supplier<BloomKFilter> supplier
    )
    {
      this.fieldNames = fieldNames;
      this.fields = fields;
      this.groupingSets = groupingSets;
      this.supplier = Preconditions.checkNotNull(supplier);
    }

    @Override
    public DimFilter rewrite(QuerySegmentWalker walker, Query parent)
    {
      return new BloomDimFilter(fieldNames, fields, groupingSets, supplier.get().serialize());
    }
  }
}
