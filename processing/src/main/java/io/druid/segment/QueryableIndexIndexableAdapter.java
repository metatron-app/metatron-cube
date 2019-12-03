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

package io.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.logger.Logger;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnAccess;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedIterable;
import io.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 */
public class QueryableIndexIndexableAdapter implements IndexableAdapter
{
  private static final Logger log = new Logger(QueryableIndexIndexableAdapter.class);
  private final int numRows;
  private final QueryableIndex input;
  private final List<String> availableDimensions;
  private final Metadata metadata;

  public QueryableIndexIndexableAdapter(QueryableIndex input)
  {
    this.input = input;
    numRows = input.getNumRows();

    // It appears possible that the dimensions have some columns listed which do not have a DictionaryEncodedColumn
    // This breaks current logic, but should be fine going forward.  This is a work-around to make things work
    // in the current state.  This code shouldn't be needed once github tracker issue #55 is finished.
    this.availableDimensions = Lists.newArrayList();
    for (String dim : input.getAvailableDimensions()) {
      final Column col = input.getColumn(dim);

      if (col == null) {
        log.warn("Column[%s] didn't exist", dim);
      } else if (col.getDictionaryEncoding() != null) {
        availableDimensions.add(dim);
      } else {
        log.info("No dictionary on dimension[%s]", dim);
      }
    }

    this.metadata = input.getMetadata();
  }

  @Override
  public Interval getDataInterval()
  {
    return input.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public Indexed<String> getDimensionNames()
  {
    return new ListIndexed<>(availableDimensions, String.class);
  }

  @Override
  public Indexed<String> getMetricNames()
  {
    final Set<String> columns = Sets.newLinkedHashSet(input.getColumnNames());
    final HashSet<String> dimensions = Sets.newHashSet(getDimensionNames());

    return new ListIndexed<>(
        Lists.newArrayList(Sets.difference(columns, dimensions)),
        String.class
    );
  }

  @Override
  public Indexed<String> getDimValueLookup(String dimension)
  {
    final Column column = input.getColumn(dimension);

    if (column == null) {
      return null;
    }

    final DictionaryEncodedColumn dict = column.getDictionaryEncoding();

    if (dict == null) {
      return null;
    }

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
        return dict.getCardinality();
      }

      @Override
      public String get(int index)
      {
        return dict.lookupName(index);
      }

      @Override
      public int indexOf(String value)
      {
        return dict.lookupId(value);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }
    };
  }

  @Override
  public Iterable<Rowboat> getRows()
  {
    return getRows(Lists.newArrayList(getDimensionNames()), Lists.newArrayList(getMetricNames()));
  }

  @Override
  public Iterable<Rowboat> getRows(final List<String> mergedDimensions, final List<String> mergedMetrics)
  {
    return new Iterable<Rowboat>()
    {
      @Override
      public Iterator<Rowboat> iterator()
      {
        return new Iterator<Rowboat>()
        {
          final GenericColumn timestamps = input.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();
          final ColumnAccess[] metrics;

          final DictionaryEncodedColumn[] dictionaryEncodedColumns;

          int currRow = 0;
          boolean done = false;

          {
            this.dictionaryEncodedColumns = FluentIterable
                .from(getDimensionNames())
                .transform(
                    new Function<String, DictionaryEncodedColumn>()
                    {
                      @Override
                      public DictionaryEncodedColumn apply(String dimName)
                      {
                        return input.getColumn(dimName)
                                    .getDictionaryEncoding();
                      }
                    }
                ).toArray(DictionaryEncodedColumn.class);

            final Indexed<String> availableMetrics = getMetricNames();
            metrics = new ColumnAccess[availableMetrics.size()];
            for (int i = 0; i < metrics.length; ++i) {
              final Column column = input.getColumn(availableMetrics.get(i));
              final ValueType type = column.getCapabilities().getType();
              if (type.isPrimitive()) {
                metrics[i] = column.getGenericColumn();
              } else {
                metrics[i] = column.getComplexColumn();
              }
            }
          }

          final int[] dimLookup = IndexMerger.toLookupMap(getDimensionNames(), mergedDimensions);
          final int[] metricLookup = IndexMerger.toLookupMap(getMetricNames(), mergedMetrics);

          @Override
          public boolean hasNext()
          {
            final boolean hasNext = currRow < numRows;
            if (!hasNext && !done) {
              CloseQuietly.close(timestamps);
              for (ColumnAccess metric : metrics) {
                CloseQuietly.close(metric);
              }
              for (DictionaryEncodedColumn dimension : dictionaryEncodedColumns) {
                CloseQuietly.close(dimension);
              }
              done = true;
            }
            return hasNext;
          }

          @Override
          public Rowboat next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            final int[][] dims = new int[dimLookup.length][];
            for (int i = 0; i < dictionaryEncodedColumns.length; i++) {
              DictionaryEncodedColumn dict = dictionaryEncodedColumns[i];
              int[] theVals;
              if (dict.hasMultipleValues()) {
                IndexedInts dimVals = dict.getMultiValueRow(currRow);
                theVals = new int[dimVals.size()];
                for (int j = 0; j < theVals.length; ++j) {
                  theVals[j] = dimVals.get(j);
                }
              } else {
                theVals = new int[] { dict.getSingleValueRow(currRow) };
              }
              dims[dimLookup[i]] = theVals;
            }

            final Object[] metricArray = new Object[metricLookup.length];
            for (int i = 0; i < metricArray.length; ++i) {
              metricArray[metricLookup[i]] = metrics[i].getValue(currRow);
            }

            final long timestamp = timestamps.getLong(currRow);
            return new Rowboat(timestamp, dims, metricArray, currRow++);
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @VisibleForTesting
  ImmutableBitmap getBitmap(String dimension, String value)
  {
    final Column column = input.getColumn(dimension);
    if (column == null) {
      return null;
    }
    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return null;
    }
    return bitmaps.getBitmap(bitmaps.getIndex(value));
  }

  @Override
  public ValueDesc getMetricType(String metric)
  {
    final Column column = input.getColumn(metric);
    if (column == null) {
      return null;
    }
    ColumnCapabilities capabilities = column.getCapabilities();
    if (!capabilities.getType().isPrimitive()) {
      return column.getComplexColumn().getType();
    }
    return ValueDesc.of(column.getCapabilities().getType());
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return input.getColumn(column).getCapabilities();
  }

  @Override
  @Nullable
  public ImmutableBitmap getBitmap(String dimension, int dictId)
  {
    final Column column = input.getColumn(dimension);
    if (column == null) {
      return null;
    }
    final BitmapIndex bitmaps = column.getBitmapIndex();
    if (bitmaps == null) {
      return null;
    }
    return bitmaps.getBitmap(dictId);
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }
}
