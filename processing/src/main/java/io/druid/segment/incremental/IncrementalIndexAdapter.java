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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.Metadata;
import io.druid.segment.Rowboat;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import io.druid.segment.incremental.IncrementalIndex.SortedDimLookup;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class IncrementalIndexAdapter implements IndexableAdapter
{
  private final Interval dataInterval;
  private final IncrementalIndex index;

  private final Map<String, DimensionIndexer> indexerMap;

  @SuppressWarnings("unchecked")
  private class DimensionIndexer
  {
    private boolean hasNull;
    private final int nullIndex;
    private final DimensionDesc dimensionDesc;
    private final MutableBitmap[] invertedIndexes;

    private SortedDimLookup dimLookup;

    public DimensionIndexer(DimensionDesc dimensionDesc)
    {
      this.dimensionDesc = dimensionDesc;
      this.invertedIndexes = new MutableBitmap[dimensionDesc.getValues().size() + 1];
      this.nullIndex = dimensionDesc.getValues().getId(null);
      this.hasNull = nullIndex >= 0;
    }

    private IncrementalIndex.DimDim getDimValues()
    {
      return dimensionDesc.getValues();
    }

    private SortedDimLookup getDimLookup()
    {
      if (dimLookup == null) {
        final IncrementalIndex.DimDim dimDim = dimensionDesc.getValues();
        if (nullIndex < 0 && hasNull) {
          dimDim.add(null);
        }
        dimLookup = dimDim.sort();
      }
      return dimLookup;
    }
  }

  public IncrementalIndexAdapter(Interval dataInterval, IncrementalIndex index, BitmapFactory bitmapFactory)
  {
    this.dataInterval = dataInterval;
    this.index = index;

    /* Sometimes it's hard to tell whether one dimension contains a null value or not.
     * If one dimension had show a null or empty value explicitly, then yes, it contains
     * null value. But if one dimension's values are all non-null, it still early to say
     * this dimension does not contain null value. Consider a two row case, first row had
     * "dimA=1" and "dimB=2", the second row only had "dimA=3". To dimB, its value are "2" and
     * never showed a null or empty value. But when we combines these two rows, dimB is null
     * in row 2. So we should iterate all rows to determine whether one dimension contains
     * a null value.
     */
    this.indexerMap = Maps.newHashMap();

    final DimensionDesc[] dimensions = index.getDimensions().toArray(new DimensionDesc[0]);
    final DimensionIndexer[] indexers = new DimensionIndexer[dimensions.length];
    for (int i = 0; i < dimensions.length; i++) {
      indexers[i] = new DimensionIndexer(dimensions[i]);
    }

    int rowNum = 0;
    for (Map.Entry<IncrementalIndex.TimeAndDims, Object[]> fact : index.getAll()) {
      final int[][] dims = fact.getKey().getDims();

      for (int i = 0; i < dimensions.length; i++) {
        final int dimIndex = dimensions[i].getIndex();
        if (dimIndex >= dims.length || dims[dimIndex] == null || dims[dimIndex].length == 0) {
          indexers[i].hasNull = true;
          continue;
        }
        final MutableBitmap[] bitmapIndexes = indexers[i].invertedIndexes;
        for (int id : dims[dimIndex]) {
          if (bitmapIndexes[id] == null) {
            bitmapIndexes[id] = bitmapFactory.makeEmptyMutableBitmap();
          }
          bitmapIndexes[id].add(rowNum);
        }
      }

      ++rowNum;
    }
    for (DimensionIndexer indexer : indexers) {
      indexerMap.put(indexer.dimensionDesc.getName(), indexer);
    }
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public Indexed<String> getDimensionNames()
  {
    return new ListIndexed<String>(index.getDimensionNames(), String.class);
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    return new ListIndexed<String>(index.getMetricNames(), String.class);
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    final DimensionIndexer indexer = indexerMap.get(dimension);
    if (indexer == null) {
      return null;
    }
    final IncrementalIndex.DimDim dimDim = indexer.getDimValues();
    final SortedDimLookup dimLookup = indexer.getDimLookup();

    return new Indexed<String>()
    {
      @Override
      public Class<? extends String> getClazz()
      {
        return String.class;
      }

      @Override
      public int size()
      {
        return dimLookup.size();
      }

      @Override
      public String get(int index)
      {
        return Objects.toString(dimLookup.getValueFromSortedId(index), null);
      }

      @Override
      public int indexOf(String value)
      {
        final int id = dimDim.getId(value);
        return id < 0 ? -1 : dimLookup.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public Iterable<Rowboat> getRows(int indexNum)
  {
    return getRows(indexNum, Lists.newArrayList(getDimensionNames()), Lists.newArrayList(getMetricNames()));
  }

  @Override
  public Iterable<Rowboat> getRows(
      final int indexNum,
      final List<String> mergedDimensions,
      final List<String> mergedMetrics
  )
  {
    return new Iterable<Rowboat>()
    {
      @Override
      public Iterator<Rowboat> iterator()
      {
        final List<DimensionDesc> dimensions = index.getDimensions();
        final SortedDimLookup[] sortedDimLookups = new SortedDimLookup[dimensions.size()];
        for (DimensionDesc dimension : dimensions) {
          sortedDimLookups[dimension.getIndex()] = indexerMap.get(dimension.getName()).getDimLookup();
        }
        final int[] dimLookup = IndexMerger.toLookupMap(getDimensionNames(), mergedDimensions);
        final int[] metricLookup = IndexMerger.toLookupMap(getMetricNames(), mergedMetrics);

        final Aggregator[] aggregators = index.getAggregators();
        /*
         * Note that the transform function increments a counter to determine the rowNum of
         * the iterated Rowboats. We need to return a new iterator on each
         * iterator() call to ensure the counter starts at 0.
         */
        return Iterators.transform(
            index.getAll().iterator(),
            new Function<Map.Entry<IncrementalIndex.TimeAndDims, Object[]>, Rowboat>()
            {
              int count = 0;

              @Override
              public Rowboat apply(Map.Entry<IncrementalIndex.TimeAndDims, Object[]> input)
              {
                final IncrementalIndex.TimeAndDims timeAndDims = input.getKey();
                final int[][] dimValues = timeAndDims.getDims();

                final int[][] dims = new int[mergedDimensions.size()][];
                for (DimensionDesc dimension : dimensions) {
                  final int dimIndex = dimension.getIndex();
                  if (dimIndex >= dimValues.length || dimValues[dimIndex] == null) {
                    continue;
                  }
                  final int[] dimValue = dimValues[dimIndex];
                  final int[] dim = new int[dimValue.length];
                  for (int i = 0; i < dim.length; ++i) {
                    dim[i] = sortedDimLookups[dimIndex].getSortedIdFromUnsortedId(dimValue[i]);
                  }
                  dims[dimLookup[dimIndex]] = dim;
                }

                final Object[] values = input.getValue();
                final Object[] metrics = new Object[metricLookup.length];
                for (int i = 0; i < aggregators.length; i++) {
                  metrics[metricLookup[i]] = aggregators[i].get(values[i]);
                }

                return new Rowboat(
                    timeAndDims.getTimestamp(),
                    dims,
                    metrics,
                    indexNum,
                    count++
                );
              }
            }
        );
      }
    };
  }

  @Override
  @Nullable
  public ImmutableBitmap getBitmap(String dimension, int index)
  {
    final DimensionIndexer accessor = indexerMap.get(dimension);
    if (accessor == null) {
      return null;
    }
    final SortedDimLookup dimLookup = accessor.getDimLookup();
    final int id = dimLookup.getUnsortedIdFromSortedId(index);
    if (id < 0 || id >= dimLookup.size()) {
      return null;
    }
    return accessor.invertedIndexes[id];
  }

  @Override
  public ValueDesc getMetricType(String metric)
  {
    return index.getMetricType(metric);
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return index.getCapabilities(column);
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
