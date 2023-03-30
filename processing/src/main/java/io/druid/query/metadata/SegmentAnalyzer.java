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

package io.druid.query.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.granularity.Granularities;
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
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.data.IndexedInts;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class SegmentAnalyzer
{
  public static List<Pair<String, ColumnAnalysis>> analyze(Segment segment, SegmentMetadataQuery query)
  {
    Preconditions.checkNotNull(segment, "segment");
    final EnumSet<AnalysisType> analysisTypes = query.getAnalysisTypes();
    // index is null for incremental-index-based segments, but storageAdapter is always available
    final QueryableIndex index = segment.asQueryableIndex(false);
    final StorageAdapter adapter = segment.asStorageAdapter(false);

    final RowResolver resolver = RowResolver.of(segment, query.getVirtualColumns());

    final List<String> columns;
    if (GuavaUtils.isNullOrEmpty(query.getColumns())) {
      final ColumnIncluderator predicate = query.getToInclude();
      if (predicate == null) {
        columns = Lists.newArrayList(resolver.getAllColumnNames());
      } else {
        columns = Lists.newArrayList();
        for (String columnName : resolver.getAllColumnNames()) {
          if (predicate.include(columnName)) {
            columns.add(columnName);
          }
        }
      }
    } else {
      columns = query.getColumns();
    }

    final List<Pair<String, ColumnAnalysis>> analysisMap = Lists.newArrayList();
    for (String columnName : columns) {
      ValueDesc valueDesc = resolver.resolve(columnName);
      if (valueDesc == null) {
        continue;
      }
      Supplier<Column> column = Suppliers.memoize(() -> index == null ? null : index.getColumn(columnName));

      ColumnAnalysis analysis;
      if (valueDesc.isDimension()) {
        analysis = analyzeDimensionColumn(column, columnName, valueDesc, resolver, adapter, analysisTypes);
      } else {
        analysis = analyzeSimpleColumn(column, columnName, valueDesc, resolver, adapter, analysisTypes);
      }
      analysisMap.add(Pair.of(columnName, analysis));
    }
    return analysisMap;
  }

  private static ColumnAnalysis analyzeSimpleColumn(
      final Supplier<Column> supplier,
      final String columnName,
      final ValueDesc valueDesc,
      final RowResolver resolver,
      final StorageAdapter storageAdapter,
      final EnumSet<AnalysisType> analysisTypes
  )
  {
    final boolean analyzingMinMax = analysisTypes.contains(AnalysisType.MINMAX);
    final boolean analyzingNullCount = analysisTypes.contains(AnalysisType.NULL_COUNT);

    int nullCount = -1;
    Comparable minValue = null;
    Comparable maxValue = null;
    boolean minMaxEvaluated = false;
    if (analyzingMinMax || analyzingNullCount) {
      final Map<String, Object> stats = storageAdapter.getColumnStats(columnName);
      if (analyzingMinMax && stats != null) {
        minValue = (Comparable) stats.get("min");
        maxValue = (Comparable) stats.get("max");
        minMaxEvaluated = stats.containsKey("min") && stats.containsKey("max");
      }
      if (analyzingNullCount && stats != null) {
        if (stats.containsKey("numNulls")) {
          nullCount = (Integer) stats.get("numNulls");
        } else if (stats.containsKey("numZeros")) {
          nullCount = (Integer) stats.get("numZeros");
        }
      }
    }
    if (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0) {
      final Column column = supplier.get();
      final ColumnCapabilities capabilities = column == null ? null : column.getCapabilities();
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
    if (valueDesc.isPrimitive() && (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0)) {
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
    ColumnMeta columnMeta = storageAdapter.getColumnMeta(columnName);

    return new ColumnAnalysis(
        valueDesc.typeName(),
        columnMeta == null ? null : columnMeta.getDescs(),
        columnMeta == null ? null : columnMeta.isHasMultipleValues(),
        serializedSize,
        null,
        nullCount,
        minValue,
        maxValue,
        null
    );
  }

  private static ColumnAnalysis analyzeDimensionColumn(
      final Supplier<Column> supplier,
      final String columnName,
      final ValueDesc valueDesc,
      final RowResolver resolver,
      final StorageAdapter storageAdapter,
      final EnumSet<AnalysisType> analysisTypes
  )
  {
    Preconditions.checkArgument(ValueDesc.isDimension(valueDesc));
    final ValueType valueType = valueDesc.subElement(ValueDesc.STRING).type();

    final boolean analyzingMinMax = analysisTypes.contains(AnalysisType.MINMAX);
    final boolean analyzingNullCount = analysisTypes.contains(AnalysisType.NULL_COUNT);

    int nullCount = -1;
    Comparable minValue = null;
    Comparable maxValue = null;
    boolean minMaxEvaluated = false;
    if (analyzingMinMax || analyzingNullCount) {
      final Map<String, Object> stats = storageAdapter.getColumnStats(columnName);
      if (stats != null && analyzingMinMax) {
        minValue = (Comparable) stats.get("min");
        maxValue = (Comparable) stats.get("max");
        minMaxEvaluated = stats.containsKey("min") && stats.containsKey("max");
      }
      if (stats != null && analyzingNullCount) {
        if (stats.containsKey("numNulls")) {
          nullCount = (Integer) stats.get("numNulls");
        } else if (stats.containsKey("numZeros")) {
          nullCount = (Integer) stats.get("numZeros");
        }
      }
    }
    if (analyzingMinMax && !minMaxEvaluated || analyzingNullCount && nullCount < 0) {
      final Column column = supplier.get();
      final ColumnCapabilities capabilities = column == null ? null : column.getCapabilities();
      if (capabilities != null && capabilities.hasBitmapIndexes()) {
        BitmapIndex bitmapIndex = column.getBitmapIndex();
        int cardinality = bitmapIndex.getCardinality();
        if (analyzingMinMax && cardinality > 0) {
          minValue = Strings.nullToEmpty(bitmapIndex.getValue(0));
          maxValue = Strings.nullToEmpty(bitmapIndex.getValue(cardinality - 1));
        }
        if (analyzingNullCount && cardinality > 0) {
          int index = bitmapIndex.getValue(0) == null ? 0 : -1;
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
                      Object comparable = id == nullIndex ? "" : selector.lookupName(id);
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

    ColumnMeta columnMeta = storageAdapter.getColumnMeta(columnName);

    return new ColumnAnalysis(
        valueDesc.typeName(),
        columnMeta == null ? null : columnMeta.getDescs(),
        columnMeta == null ? null : columnMeta.isHasMultipleValues(),
        serializedSize,
        cardinality < 0 ? null : new long[] {cardinality, cardinality},
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
    return storageAdapter.makeCursors(null, null, resolver, Granularities.ALL, false, null)
                         .accumulate(initial, accumulator);
  }
}
