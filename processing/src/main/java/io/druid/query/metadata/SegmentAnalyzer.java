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

package io.druid.query.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Accumulator;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.QueryGranularities;
import io.druid.query.BaseQuery;
import io.druid.query.RowResolver;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery.AnalysisType;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.Map;

public class SegmentAnalyzer
{
  public static Map<String, ColumnAnalysis> analyze(Segment segment, SegmentMetadataQuery query)
  {
    Preconditions.checkNotNull(segment, "segment");
    final EnumSet<AnalysisType> analysisTypes = query.getAnalysisTypes();
    // index is null for incremental-index-based segments, but storageAdapter is always available
    final QueryableIndex index = segment.asQueryableIndex(false);
    final StorageAdapter adapter = segment.asStorageAdapter(false);

    final RowResolver resolver = RowResolver.of(segment, BaseQuery.getVirtualColumns(query));

    final ColumnIncluderator predicate = query.getToInclude();

    final Map<String, ColumnAnalysis> columns = Maps.newTreeMap();
    for (String columnName : resolver.getAllColumnNames()) {
      if (!predicate.include(columnName)) {
        continue;
      }
      ValueDesc valueDesc = resolver.resolveColumn(columnName);
      if (valueDesc == null) {
        continue;
      }
      Column column = index == null ? null : index.getColumn(columnName);

      ColumnAnalysis analysis;
      if (ValueDesc.isDimension(valueDesc)) {
        analysis = analyzeDimensionColumn(column, columnName, valueDesc, resolver, adapter, analysisTypes);
      } else {
        analysis = analyzeSimpleColumn(column, columnName, valueDesc, resolver, adapter, analysisTypes);
      }
      columns.put(columnName, analysis);
    }
    return columns;
  }

  private static ColumnAnalysis analyzeSimpleColumn(
      final Column column,
      final String columnName,
      final ValueDesc valueDesc,
      final RowResolver resolver,
      final StorageAdapter storageAdapter,
      final EnumSet<AnalysisType> analysisTypes
  )
  {
    final Map<String, Object> stats = column == null ? null : column.getColumnStats();
    final ColumnCapabilities capabilities = column == null ? null : column.getCapabilities();

    final boolean analyzingMinMax = analysisTypes.contains(AnalysisType.MINMAX);
    final boolean analyzingNullCount = analysisTypes.contains(AnalysisType.NULL_COUNT);

    int nullCount = -1;
    Comparable minValue = null;
    Comparable maxValue = null;
    boolean minMaxEvaluated = false;
    if ((analyzingMinMax || analyzingNullCount) && stats != null) {
      if (analyzingMinMax) {
        minValue = (Comparable) stats.get("min");
        maxValue = (Comparable) stats.get("max");
        minMaxEvaluated = stats.containsKey("min") && stats.containsKey("max");
      }
      if (analyzingNullCount && stats.containsKey("numZeros")) {
        nullCount = (Integer) stats.get("numZeros");
      }
    }
    if (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0) {
      if (capabilities != null && capabilities.hasMetricBitmap()) {
        HistogramBitmap metricBitmap = column.getMetricBitmap();
        if (analyzingMinMax) {
          minValue = metricBitmap.getMin();
          maxValue = metricBitmap.getMax();
          minMaxEvaluated = true;
        }
        if (analyzingNullCount && nullCount < 0) {
          nullCount = metricBitmap.zeroRows();
        }
      }
    }
    if (ValueDesc.isPrimitive(valueDesc) &&
        (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0)) {
      Object[] accumulated = accumulate(
          storageAdapter, resolver,
          new Object[]{null, null, new MutableInt()}, new Accumulator<Object[], Cursor>()
          {
            @Override
            @SuppressWarnings("unchecked")
            public Object[] accumulate(Object[] accumulated, Cursor cursor)
            {
              final ObjectColumnSelector selector = cursor.makeObjectColumnSelector(columnName);
              if (selector == null) {
                for (; !cursor.isDone(); cursor.advance()) {
                  ((MutableInt) accumulated[2]).increment();
                }
              } else {
                Preconditions.checkArgument(valueDesc.equals(selector.type()), valueDesc + " vs " + selector.type());
                for (; !cursor.isDone(); cursor.advance()) {
                  Comparable comparable = (Comparable) selector.get();
                  if (comparable == null) {
                    ((MutableInt) accumulated[2]).increment();
                    continue;
                  }
                  if (accumulated[0] == null || comparable.compareTo(accumulated[0]) < 0) {
                    accumulated[0] = comparable;
                  }
                  if (accumulated[1] == null || comparable.compareTo(accumulated[1]) > 0) {
                    accumulated[1] = comparable;
                  }
                }
              }
              return accumulated;
            }
          }
      );
      if (analyzingMinMax) {
        minValue = (Comparable) accumulated[0];
        maxValue = (Comparable) accumulated[1];
      }
      if (analyzingNullCount) {
        nullCount = ((MutableInt) accumulated[2]).intValue();
      }
    }
    long serializedSize = -1;
    if (analysisTypes.contains(AnalysisType.SERIALIZED_SIZE)) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }

    return new ColumnAnalysis(
        valueDesc.typeName(),
        false,
        serializedSize,
        -1,
        nullCount,
        minValue,
        maxValue,
        null
    );
  }

  private static ColumnAnalysis analyzeDimensionColumn(
      final Column column,
      final String columnName,
      final ValueDesc valueDesc,
      final RowResolver resolver,
      final StorageAdapter storageAdapter,
      final EnumSet<AnalysisType> analysisTypes
  )
  {
    Preconditions.checkArgument(ValueDesc.isDimension(valueDesc));
    final ValueType valueType = valueDesc.subElement().type();
    final Map<String, Object> stats = column == null ? null : column.getColumnStats();
    final ColumnCapabilities capabilities = column == null ? null : column.getCapabilities();

    final boolean analyzingMinMax = analysisTypes.contains(AnalysisType.MINMAX);
    final boolean analyzingNullCount = analysisTypes.contains(AnalysisType.NULL_COUNT);

    int nullCount = -1;
    Comparable minValue = null;
    Comparable maxValue = null;
    boolean minMaxEvaluated = false;
    if ((analyzingMinMax || analyzingNullCount) && stats != null) {
      if (analyzingMinMax) {
        minValue = (Comparable) stats.get("min");
        maxValue = (Comparable) stats.get("max");
        minMaxEvaluated = stats.containsKey("min") && stats.containsKey("max");
      }
      if (analyzingNullCount && stats.containsKey("numZeros")) {
        nullCount = (Integer) stats.get("numZeros");
      }
    }
    if (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0) {
      if (capabilities != null && capabilities.hasBitmapIndexes()) {
        BitmapIndex bitmapIndex = column.getBitmapIndex();
        int cardinality = bitmapIndex.getCardinality();
        if (analyzingMinMax && cardinality > 0) {
          minValue = Strings.nullToEmpty(bitmapIndex.getValue(0));
          maxValue = Strings.nullToEmpty(bitmapIndex.getValue(cardinality - 1));
        }
        if (analyzingNullCount && cardinality > 0) {
          int index = bitmapIndex.getIndex(null);
          if (index >= 0) {
            nullCount = bitmapIndex.getBitmap(index).size();
          }
        }
      } else {
        final Comparator comparator = valueType.comparator();
        Object[] accumulated = accumulate(
            storageAdapter, resolver,
            new Object[]{null, null, new MutableInt()},
            new Accumulator<Object[], Cursor>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Object[] accumulate(final Object[] accumulated, Cursor cursor)
              {
                final DimensionSelector selector = cursor.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
                if (selector == null) {
                  for (; !cursor.isDone(); cursor.advance()) {
                    ((MutableInt) accumulated[2]).increment();
                  }
                } else {
                  final int nullIndex = selector.lookupId(null);
                  for (; !cursor.isDone(); cursor.advance()) {
                    final IndexedInts vals = selector.getRow();
                    for (int i = 0; i < vals.size(); ++i) {
                      final int id = vals.get(i);
                      if (id == nullIndex) {
                        ((MutableInt) accumulated[2]).increment();
                      }
                      Comparable comparable = id == nullIndex ? "" : selector.lookupName(id);
                      if (accumulated[0] == null || comparator.compare(comparable, accumulated[0]) < 0) {
                        accumulated[0] = comparable;
                      }
                      if (accumulated[1] == null || comparator.compare(comparable, accumulated[1]) > 0) {
                        accumulated[1] = comparable;
                      }
                    }
                  }
                }

                return accumulated;
              }
            }
        );
        if (analyzingMinMax) {
          minValue = (Comparable) accumulated[0];
          maxValue = (Comparable) accumulated[1];
        }
        if (analyzingNullCount) {
          nullCount = ((MutableInt) accumulated[2]).intValue();
        }
      }
    }
    long serializedSize = -1;
    if (analysisTypes.contains(AnalysisType.SERIALIZED_SIZE)) {
      serializedSize = storageAdapter.getSerializedSize(columnName);
    }

    int cardinality = -1;
    if (analysisTypes.contains(AnalysisType.CARDINALITY)) {
      cardinality = storageAdapter.getDimensionCardinality(columnName);
    }

    return new ColumnAnalysis(
        valueDesc.typeName(),
        storageAdapter.getColumnCapabilities(columnName).hasMultipleValues(),
        serializedSize,
        cardinality,
        nullCount,
        minValue,
        maxValue,
        null
    );
  }

  private static <T> T accumulate(
      StorageAdapter storageAdapter,
      RowResolver resolver,
      T initial,
      Accumulator<T, Cursor> accumulator
  )
  {
    Interval interval = new Interval(
        storageAdapter.getMinTime().getMillis(),
        storageAdapter.getMaxTime().getMillis() + 1
    );
    return storageAdapter.makeCursors(null, interval, resolver, QueryGranularities.ALL, null, false)
                         .accumulate(initial, accumulator);
  }
}
