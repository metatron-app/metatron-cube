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

import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.data.input.Row;
import io.druid.java.util.common.Pair;
import io.druid.query.Query;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnMeta;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.Closeable;
import java.math.BigInteger;
import java.util.Map;

/**
 */
public interface QueryableIndex extends SchemaProvider, Closeable
{
  Interval getInterval();       // [segment-start, segment-end )
  Interval getTimeMinMax();   // [min-time, max-time]
  Indexed<String> getColumnNames();
  Indexed<String> getAvailableDimensions();
  Iterable<String> getAvailableMetrics();

  Map<BigInteger, Pair<CuboidSpec, QueryableIndex>> getQuboids();
  QueryableIndex cuboidFor(Query<?> query);

  long getSerializedSize(String columnName);
  ColumnMeta getColumnMeta(String columnName);
  Column getColumn(String columnName);

  default GenericColumn.TimestampType getTimestamp()
  {
    return (GenericColumn.TimestampType) getColumn(Row.TIME_COLUMN_NAME).getGenericColumn();
  }

  default Class<? extends GenericColumn> getGenericColumnType(String columnName)
  {
    Column column = getColumn(columnName);
    return column != null && column.hasGenericColumn() ? column.getGenericColumnType() : null;
  }

  int getNumRows();
  long getSerializedSize();
  Metadata getMetadata();
  BitmapFactory getBitmapFactoryForDimensions();

  /**
   * The close method shouldn't actually be here as this is nasty. We will adjust it in the future.
   * @throws java.io.IOException if an exception was thrown closing the index
   */
  //@Deprecated // This is still required for SimpleQueryableIndex. It should not go away unitl SimpleQueryableIndex is fixed
  void close();
}
