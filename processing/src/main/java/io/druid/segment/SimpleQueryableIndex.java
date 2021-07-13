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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.Query;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex implements QueryableIndex
{
  private static final Logger LOG = new Logger(QueryableIndex.class);

  private final Interval interval;
  private final Indexed<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final Map<String, Supplier<ColumnMeta>> descriptors;
  private final Map<String, Supplier<Column>> columns;
  private final SmooshedFileMapper fileMapper;
  private final Metadata metadata;
  private final Supplier<TimeStats> stats;

  private final Map<BigInteger, Pair<CuboidSpec, QueryableIndex>> cuboids;

  public SimpleQueryableIndex(
      Interval interval,
      Indexed<String> columnNames,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnMeta>> descriptors,
      Map<String, Supplier<Column>> columns,
      Map<BigInteger, Pair<CuboidSpec, QueryableIndex>> cuboids,
      SmooshedFileMapper fileMapper,
      Metadata metadata
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.interval = interval;
    this.columnNames = columnNames;
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.descriptors = descriptors;
    this.columns = columns;
    this.cuboids = cuboids == null ? ImmutableMap.of() : cuboids;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.stats = Suppliers.memoize(() -> {
      final GenericColumn column = columns.get(Column.TIME_COLUMN_NAME).get().getGenericColumn();
      try {
        final int numRows = column.getNumRows();
        return new TimeStats(numRows, Intervals.utc(column.getLong(0), column.getLong(numRows - 1)));
      }
      finally {
        CloseQuietly.close(column);
      }
    });
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public Interval getTimeMinMax()
  {
    return stats.get().interval;
  }

  @Override
  public int getNumRows()
  {
    return stats.get().numRows;
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Sets.difference(Sets.newHashSet(columnNames), Sets.newHashSet(availableDimensions));
  }

  @Override
  public Indexed<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return bitmapFactory;
  }

  @Override
  public ColumnMeta getColumnMeta(String columnName)
  {
    if (descriptors == null) {
      final Supplier<Column> supplier = columns.get(columnName);
      return supplier == null ? null : supplier.get().getMetaData();
    }
    final Supplier<ColumnMeta> supplier = descriptors.get(columnName);
    return supplier == null ? null : supplier.get();
  }

  @Override
  public Column getColumn(String columnName)
  {
    final Supplier<Column> supplier = columns.get(columnName);
    return supplier == null ? null : supplier.get();
  }

  @Override
  public void close() throws IOException
  {
    fileMapper.close();
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Map<BigInteger, Pair<CuboidSpec, QueryableIndex>> getQuboids()
  {
    return cuboids;
  }

  @Override
  public QueryableIndex cuboidFor(Query<?> query)
  {
    if (GuavaUtils.isNullOrEmpty(cuboids) || !(query instanceof BaseAggregationQuery)) {
      return null;
    }
    final BaseAggregationQuery aggregation = (BaseAggregationQuery) query;
    if (!DimensionSpecs.isAllDefault(aggregation.getDimensions())) {
      return null;
    }
    Pair<CuboidSpec, QueryableIndex> withMinRow = null;
    for (Pair<CuboidSpec, QueryableIndex> entry : Iterables.filter(
        cuboids.values(),
        new Predicate<Pair<CuboidSpec, QueryableIndex>>()
        {
          @Override
          public boolean apply(Pair<CuboidSpec, QueryableIndex> input)
          {
            return input.lhs.supports(aggregation);
          }
        }
    )) {
      if (withMinRow == null || withMinRow.rhs.getNumRows() > entry.rhs.getNumRows()) {
        withMinRow = entry;
      }
    }
    if (withMinRow != null) {
      LOG.debug("Using cuboid... %s", withMinRow.lhs.getDimensions());
    }
    return withMinRow == null ? null : withMinRow.rhs;
  }

  @Override
  public Schema asSchema(boolean prependTime)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();
    Map<String, Map<String, String>> columnDescriptors = Maps.newHashMap();
    if (prependTime) {
      columnNames.add(Row.TIME_COLUMN_NAME);
      columnTypes.add(ValueDesc.LONG);
    }
    for (String columnName : Iterables.concat(getAvailableDimensions(), getAvailableMetrics())) {
      Column column = Preconditions.checkNotNull(getColumn(columnName), "cannot find column %s", columnName);
      columnNames.add(columnName);
      columnTypes.add(column.getType());
      ColumnCapabilities capabilities = column.getCapabilities();
      if (capabilities != null) {
        columnCapabilities.put(columnName, capabilities);
      }
      Map<String, String> descs = column.getColumnDescs();
      if (!GuavaUtils.isNullOrEmpty(descs)) {
        columnDescriptors.put(columnName, descs);
      }
    }
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.getAggregatorsFromMeta(getMetadata());
    return new Schema(columnNames, columnTypes, aggregators, columnCapabilities, columnDescriptors);
  }

  @Override
  public RowSignature asSignature(boolean prependTime)
  {
    List<String> columnNames = Lists.newArrayList();
    List<ValueDesc> columnTypes = Lists.newArrayList();
    if (prependTime) {
      columnNames.add(Row.TIME_COLUMN_NAME);
      columnTypes.add(ValueDesc.LONG);
    }
    for (String dimension : getAvailableDimensions()) {
      ColumnMeta meta = Preconditions.checkNotNull(getColumnMeta(dimension), "cannot find column %s", dimension);
      columnNames.add(dimension);
      columnTypes.add(ValueDesc.ofDimension(meta.getValueType()));
    }
    for (String metric : getAvailableMetrics()) {
      ColumnMeta meta = Preconditions.checkNotNull(getColumnMeta(metric), "cannot find column %s", metric);
      columnNames.add(metric);
      columnTypes.add(meta.getValueType());
    }
    return new RowSignature(columnNames, columnTypes);
  }

  private static class TimeStats
  {
    private final int numRows;
    private final Interval interval;

    private TimeStats(int numRows, Interval interval)
    {
      this.numRows = numRows;
      this.interval = interval;
    }
  }
}
