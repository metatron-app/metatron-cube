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

package io.druid.segment.column;

import com.google.common.base.Preconditions;
import io.druid.data.ValueType;
import io.druid.segment.ColumnPartProvider;

/**
 */
public class ColumnBuilder
{
  private ValueType type = null;
  private boolean hasMultipleValues = false;

  private ColumnPartProvider<DictionaryEncodedColumn> dictionaryEncodedColumn = null;
  private ColumnPartProvider<RunLengthColumn> runLengthColumn = null;
  private ColumnPartProvider<GenericColumn> genericColumn = null;
  private ColumnPartProvider<ComplexColumn> complexColumn = null;
  private ColumnPartProvider<BitmapIndex> bitmapIndex = null;
  private ColumnPartProvider<SpatialIndex> spatialIndex = null;

  public ColumnBuilder setType(ValueType type)
  {
    this.type = type;
    return this;
  }

  public ColumnBuilder setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  public ColumnBuilder setDictionaryEncodedColumn(ColumnPartProvider<DictionaryEncodedColumn> dictionaryEncodedColumn)
  {
    this.dictionaryEncodedColumn = dictionaryEncodedColumn;
    return this;
  }

  public ColumnBuilder setRunLengthColumn(ColumnPartProvider<RunLengthColumn> runLengthColumn)
  {
    this.runLengthColumn = runLengthColumn;
    return this;
  }

  public ColumnBuilder setGenericColumn(ColumnPartProvider<GenericColumn> genericColumn)
  {
    this.genericColumn = genericColumn;
    return this;
  }

  public ColumnBuilder setComplexColumn(ColumnPartProvider<ComplexColumn> complexColumn)
  {
    this.complexColumn = complexColumn;
    return this;
  }

  public ColumnBuilder setBitmapIndex(ColumnPartProvider<BitmapIndex> bitmapIndex)
  {
    this.bitmapIndex = bitmapIndex;
    return this;
  }

  public ColumnBuilder setSpatialIndex(ColumnPartProvider<SpatialIndex> spatialIndex)
  {
    this.spatialIndex = spatialIndex;
    return this;
  }

  public Column build()
  {
    Preconditions.checkState(type != null, "Type must be set.");

    return new SimpleColumn(
        new ColumnCapabilitiesImpl()
            .setType(type)
            .setDictionaryEncoded(dictionaryEncodedColumn != null)
            .setHasBitmapIndexes(bitmapIndex != null)
            .setHasSpatialIndexes(spatialIndex != null)
            .setRunLengthEncoded(runLengthColumn != null)
            .setHasMultipleValues(hasMultipleValues)
        ,
        dictionaryEncodedColumn,
        runLengthColumn,
        genericColumn,
        complexColumn,
        bitmapIndex,
        spatialIndex
    );
  }
}
