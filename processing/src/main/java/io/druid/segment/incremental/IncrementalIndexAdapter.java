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
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.collections.IntList;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.Metadata;
import io.druid.segment.Rowboat;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import io.druid.segment.incremental.IncrementalIndex.SortedDimLookup;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
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

  private static class DimensionIndexer implements InvertedIndexProvider
  {
    private final DimensionDesc dimensionDesc;
    private final long[] inverted;

    private SortedDimLookup dimLookup;

    public DimensionIndexer(DimensionDesc dimensionDesc, long[] inverted)
    {
      this.dimensionDesc = dimensionDesc;
      this.inverted = inverted;
    }

    private IncrementalIndex.DimDim getDimValues()
    {
      return dimensionDesc.getValues();
    }

    private SortedDimLookup getDimLookup()
    {
      if (dimLookup == null) {
        dimLookup = dimensionDesc.getValues().sort();
      }
      return dimLookup;
    }

    public Indexed<String> getDimValueLookup()
    {
      return new Indexed<String>()
      {
        private final IncrementalIndex.DimDim dimDim = dimensionDesc.getValues();
        private final SortedDimLookup dimLookup = getDimLookup();

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
        @SuppressWarnings("unchecked")
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
    public IntIterator apply(int x)
    {
      SortedDimLookup dimLookup = getDimLookup();
      int id = dimLookup.getUnsortedIdFromSortedId(x);
      if (id < 0 || id >= dimLookup.size()) {
        return null;
      }
      int ix = Arrays.binarySearch(inverted, (long) id << Integer.SIZE);

      return new IntIterators.Abstract()
      {
        private int x = ix < 0 ? -ix - 1 : ix;

        @Override
        public boolean hasNext()
        {
          return x < inverted.length && (inverted[x] >> Integer.SIZE) == id;
        }

        @Override
        public int next()
        {
          return Ints.checkedCast(inverted[x++] & Integer.MAX_VALUE);
        }
      };
    }
  }

  @SuppressWarnings("unchecked")
  public IncrementalIndexAdapter(Interval dataInterval, IncrementalIndex index, BitmapFactory bitmapFactory)
  {
    this.dataInterval = dataInterval;
    this.index = index;

    /* Sometimes it's hard to tell whether one dimension contains a null value or not.
     * If one dimension had showed a null or empty value explicitly, then yes, it contains
     * null value. But if one dimension's values are all non-null, it is still early to say
     * this dimension does not contain null value. Consider a two row case, first row had
     * "dimA=1" and "dimB=2", the second row only had "dimA=3". To dimB, its value are "2" and
     * never showed a null or empty value. But when we combine these two rows, dimB is null
     * in row 2. So we should iterate all rows to determine whether one dimension contains
     * a null value.
     */
    this.indexerMap = Maps.newHashMap();

    final DimensionDesc[] dimensions = index.getDimensions().toArray(new DimensionDesc[0]);

    final int[] dxs = new int[dimensions.length];
    final boolean[] containsNull = new boolean[dimensions.length];
    final LongArrayList[] inverted = new LongArrayList[dimensions.length];
    for (int i = 0; i < dimensions.length; i++) {
      dxs[i] = dimensions[i].getIndex();
      containsNull[i] = dimensions[i].containsNull();
      inverted[i] = new LongArrayList(index.size());
    }

    int rowNum = 0;
    for (Map.Entry<IncrementalIndex.TimeAndDims, Object[]> fact : index.getAll().entrySet()) {
      final int[][] dims = fact.getKey().getDims();

      for (int i = 0; i < dxs.length; i++) {
        final int dx = dxs[i];
        if (dx >= dims.length || dims[dx] == null || dims[dx].length == 0) {
          if (!containsNull[i]) {
            dimensions[i].getValues().add(null);
            containsNull[i] = true;
          }
          continue;
        }
        for (int id : dims[dx]) {
          inverted[dx].add(((long) id << Integer.SIZE) + rowNum);
        }
      }
      rowNum++;
    }
    for (int i = 0; i < dimensions.length; i++) {
      long[] keys = inverted[i].toLongArray();
      Arrays.sort(keys);
      indexerMap.put(dimensions[i].getName(), new DimensionIndexer(dimensions[i], keys));
    }
  }

  @Override
  public Interval getInterval()
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
    return ListIndexed.ofString(index.getDimensionNames());
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    return ListIndexed.ofString(index.getMetricNames());
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    DimensionIndexer indexer = indexerMap.get(dimension);
    return indexer == null ? null : indexer.getDimValueLookup();
  }

  @Override
  public InvertedIndexProvider getInvertedIndex(String dimension)
  {
    return indexerMap.get(dimension);
  }

  @Override
  public Iterable<Rowboat> getRows(final List<String> mergedDimensions, final List<String> mergedMetrics)
  {
    return new Iterable<Rowboat>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Iterator<Rowboat> iterator()
      {
        final List<DimensionDesc> dimensions = index.getDimensions();
        final SortedDimLookup[] sortedDimLookups = new SortedDimLookup[dimensions.size()];
        for (DimensionDesc dimension : dimensions) {
          sortedDimLookups[dimension.getIndex()] = indexerMap.get(dimension.getName()).getDimLookup();
        }
        final int[] dimLookup = IndexMerger.toLookupMap(getDimensionNames(), mergedDimensions);
        final int[] metricLookup = IndexMerger.toLookupMap(getMetricNames(), mergedMetrics);

        final Aggregator[] aggregators = index.getMetricAggregators();
        /*
         * Note that the transform function increments a counter to determine the rowNum of
         * the iterated Rowboats. We need to return a new iterator on each
         * iterator() call to ensure the counter starts at 0.
         */
        return Iterators.transform(
            index.getAll().entrySet().iterator(),
            new Function<Map.Entry<IncrementalIndex.TimeAndDims, Object[]>, Rowboat>()
            {
              private int rownum;

              @Override
              public Rowboat apply(Map.Entry<IncrementalIndex.TimeAndDims, Object[]> input)
              {
                final IncrementalIndex.TimeAndDims timeAndDims = input.getKey();
                final int[][] dimValues = timeAndDims.getDims();

                final Object[] values = input.getValue();
                final int[][] dims = new int[mergedDimensions.size()][];
                for (DimensionDesc dimension : dimensions) {
                  final int dimIndex = dimension.getIndex();
                  if (dimIndex >= dimValues.length) {
                    continue;
                  }
                  final int pivotIx = dimension.getPivotIndex();
                  if (pivotIx >= 0) {
                    final int[] indices = ((IntList) values[pivotIx]).array();
                    for (int i = 0; i < indices.length; i++) {
                      indices[i] = sortedDimLookups[dimIndex].getSortedIdFromUnsortedId(indices[i]);
                    }
                    final MultiValueHandling handling = dimension.getMultiValueHandling();
                    if (indices.length > 1 && handling != MultiValueHandling.ARRAY) {
                      dims[dimLookup[dimIndex]] = handling.rewrite(IncrementalIndex.sort(indices));
                    } else {
                      dims[dimLookup[dimIndex]] = indices;
                    }
                  } else if (dimValues[dimIndex] != null) {
                    final int[] dimValue = dimValues[dimIndex];
                    final int[] indices = new int[dimValue.length];
                    for (int i = 0; i < indices.length; i++) {
                      indices[i] = sortedDimLookups[dimIndex].getSortedIdFromUnsortedId(dimValue[i]);
                    }
                    dims[dimLookup[dimIndex]] = indices;
                  }
                }

                final Object[] metrics = new Object[metricLookup.length];
                for (int i = 0; i < aggregators.length; i++) {
                  if (aggregators[i] != null) {
                    metrics[metricLookup[i]] = aggregators[i].get(values[i]);
                  }
                }

                return new Rowboat(timeAndDims.getTimestamp(), dims, metrics, rownum++);
              }
            }
        );
      }
    };
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
