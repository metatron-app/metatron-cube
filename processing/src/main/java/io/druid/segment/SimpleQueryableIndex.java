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

package io.druid.segment;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.query.select.Schema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final Indexed<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final Map<String, Column> columns;
  private final SmooshedFileMapper fileMapper;
  private final Metadata metadata;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> columnNames,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Column> columns,
      SmooshedFileMapper fileMapper,
      Metadata metadata
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.dataInterval = dataInterval;
    this.columnNames = columnNames;
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return columns.get(Column.TIME_COLUMN_NAME).getLength();
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
  public Column getColumn(String columnName)
  {
    return columns.get(columnName);
  }

  @Override
  public ValueDesc getColumnType(String columnName)
  {
    Column column = columns.get(columnName);
    if (column == null) {
      return null;
    }
    ColumnCapabilities capabilities = column.getCapabilities();
    ValueType valueType = capabilities.getType();
    if (capabilities.isDictionaryEncoded()) {
      return ValueDesc.ofDimension(valueType);
    } else if (!valueType.isPrimitive()) {
      return column.getComplexColumn().getType();
    }
    return ValueDesc.of(valueType);

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
  public Schema asSchema(boolean prependTime)
  {
    List<String> dimensionNames = prependTime ? GuavaUtils.concat(Row.TIME_COLUMN_NAME, getAvailableDimensions()) :
                                  Lists.newArrayList(getAvailableDimensions());
    List<String> metricNames = Lists.newArrayList(getAvailableMetrics());
    List<ValueDesc> columnTypes = Lists.newArrayList();

    Map<String, ColumnCapabilities> columnCapabilities = Maps.newHashMap();
    Map<String, Map<String, String>> columnDescriptors = Maps.newHashMap();
    for (String columnName : Iterables.concat(dimensionNames, metricNames)) {
      Column column = getColumn(columnName);
      columnTypes.add(getColumnType(columnName));
      columnCapabilities.put(columnName, column.getCapabilities());
      Map<String, String> descs = column.getColumnDescs();
      if (!GuavaUtils.isNullOrEmpty(descs)) {
        columnDescriptors.put(columnName, descs);
      }
    }
    Map<String, AggregatorFactory> aggregators = AggregatorFactory.getAggregatorsFromMeta(metadata);
    return new Schema(dimensionNames, metricNames, columnTypes, aggregators, columnCapabilities, columnDescriptors);
  }
}
