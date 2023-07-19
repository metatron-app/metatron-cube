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

package io.druid.segment.column;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IntValues;
import io.druid.segment.data.IntsValues;
import io.druid.segment.serde.DictionaryEncodedColumnSupplier;

import java.util.List;
import java.util.Map;

/**
 */
public class ColumnBuilder
{
  private final String name;
  private ValueDesc type = null;
  private int numRows = -1;
  private boolean hasMultipleValues = false;

  private ExternalIndexProvider<FSTHolder> fstIndex = null;
  private ColumnPartProvider<RunLengthColumn> runLengthColumn = null;
  private ColumnPartProvider<GenericColumn> genericColumn = null;
  private ColumnPartProvider<ComplexColumn> complexColumn = null;
  private ColumnPartProvider<BitmapIndex> bitmapIndex = null;
  private ColumnPartProvider<SpatialIndex> spatialIndex = null;
  private ColumnPartProvider<HistogramBitmap> metricBitmap = null;
  private ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap = null;
  private final Map<Class, ExternalIndexProvider> secondaryIndex = Maps.newHashMap();

  private final DictionaryEncodedColumnSupplier.Builder builder = new DictionaryEncodedColumnSupplier.Builder();

  private Map<String, Object> stats;
  private Map<String, String> descs;

  public ColumnBuilder(String name)
  {
    this.name = Preconditions.checkNotNull(name, "name must be set");
  }

  public String getName()
  {
    return name;
  }

  public boolean isTimestamp()
  {
    return Row.TIME_COLUMN_NAME.equalsIgnoreCase(name);
  }

  public ColumnBuilder setType(ValueDesc type)
  {
    this.type = type;
    return this;
  }

  private <T> ColumnPartProvider<T> setNumRows(ColumnPartProvider<T> provider)
  {
    setNumRows(provider.numRows());
    return provider;
  }

  private void setNumRows(int size)
  {
    Preconditions.checkArgument(numRows < 0 || numRows == size);
    this.numRows = size;
  }

  public ColumnBuilder setHasMultipleValues(boolean hasMultipleValues)
  {
    this.hasMultipleValues = hasMultipleValues;
    return this;
  }

  public ColumnBuilder setDictionary(ColumnPartProvider<Dictionary<String>> dictionary)
  {
    builder.dictionary = dictionary;
    return this;
  }

  // hack for cube
  public ColumnPartProvider<Dictionary<String>> getDictionary()
  {
    return builder.dictionary;
  }

  public ColumnBuilder setFST(ExternalIndexProvider<FSTHolder> fstIndex)
  {
    this.fstIndex = fstIndex;
    return this;
  }

  public ColumnBuilder setMultiValuedColumn(ColumnPartProvider<IntsValues> mvColumn)
  {
    setNumRows(mvColumn.numRows());
    builder.multiValuedColumn = mvColumn;
    return this;
  }


  public ColumnBuilder setSingleValuedColumn(ColumnPartProvider<IntValues> svColumn)
  {
    setNumRows(svColumn.numRows());
    builder.singleValuedColumn = svColumn;
    return this;
  }

  public ColumnBuilder setRunLengthColumn(ColumnPartProvider<RunLengthColumn> runLengthColumn)
  {
    this.runLengthColumn = setNumRows(runLengthColumn);
    return this;
  }

  public ColumnBuilder setGenericColumn(ColumnPartProvider<GenericColumn> genericColumn)
  {
    this.genericColumn = setNumRows(genericColumn);
    return this;
  }

  public ColumnBuilder setComplexColumn(ColumnPartProvider<ComplexColumn> complexColumn)
  {
    this.complexColumn = setNumRows(complexColumn);
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

  public ColumnBuilder setMetricBitmap(ColumnPartProvider<HistogramBitmap> metricBitmap)
  {
    this.metricBitmap = metricBitmap;
    return this;
  }

  public ColumnBuilder setBitSlicedBitmap(ColumnPartProvider<BitSlicedBitmap> bitSlicedBitmap)
  {
    this.bitSlicedBitmap = bitSlicedBitmap;
    return this;
  }

  public ColumnBuilder addSecondaryIndex(ExternalIndexProvider<? extends SecondaryIndex> secondaryIndex)
  {
    this.secondaryIndex.put(secondaryIndex.provides(), secondaryIndex);
    return this;
  }

  public ColumnBuilder setColumnStats(Map<String, Object> stats)
  {
    this.stats = stats;
    return this;
  }

  public ColumnBuilder setColumnDescs(Map<String, String> descs)
  {
    this.descs = descs;
    return this;
  }

  public String getColumnDesc(String indexName)
  {
    return descs == null ? null : descs.get(indexName);
  }

  public ValueDesc getType()
  {
    return type;
  }

  public int getNumRows()
  {
    return numRows;
  }

  public Column build()
  {
    Preconditions.checkNotNull(type, "type must be set");

    List<String> indexNames = secondaryIndex.isEmpty() ? null : GuavaUtils.transform(secondaryIndex.values(), x -> x.source());
    ColumnPartProvider.DictionarySupport dimension = builder.build();
    return new SimpleColumn(
        name,
        new ColumnCapabilities()
            .setType(type.type())
            .setTypeName(type.typeName())
            .setDictionaryEncoded(dimension != null)
            .setHasDictionaryFST(fstIndex != null)
            .setHasBitmapIndexes(bitmapIndex != null)
            .setHasMetricBitmap(metricBitmap != null)
            .setHasBitSlicedBitmap(bitSlicedBitmap != null)
            .setHasSpatialIndexes(spatialIndex != null)
            .setRunLengthEncoded(runLengthColumn != null)
            .setHasMultipleValues(hasMultipleValues)
            .setExternalIndices(indexNames)
        ,
        dimension,
        fstIndex,
        runLengthColumn,
        genericColumn,
        complexColumn,
        bitmapIndex,
        spatialIndex,
        metricBitmap,
        bitSlicedBitmap,
        secondaryIndex,
        stats,
        descs
    );
  }
}
