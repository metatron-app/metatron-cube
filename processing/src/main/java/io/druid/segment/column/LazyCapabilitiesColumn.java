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

import com.google.common.base.Supplier;
import io.druid.segment.ExternalIndexProvider;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.Dictionary;

import java.util.Map;
import java.util.Set;

/**
 * A {@link Column} whose {@link ColumnCapabilities} and metadata are known up front (carried in the range
 * container's readable header) but whose actual data is built lazily on first value access. This lets
 * {@code getColumnCapabilities}/{@code asSignature}/{@code asSchema} be answered WITHOUT fetching+decoding the
 * column payload — the whole point of the header front-index — so a query only pays the range fetch for columns
 * it actually reads. Any data method realizes the (memoized) {@code delegate}, which fetches the column's bytes
 * and runs the normal v9 {@code ColumnDescriptor.read(...)}.
 */
public class LazyCapabilitiesColumn implements Column
{
  private final String name;
  private final ColumnCapabilities capabilities;
  private final Supplier<Column> delegate;   // memoized: fetch + decode on first data access

  public LazyCapabilitiesColumn(String name, ColumnCapabilities capabilities, Supplier<Column> delegate)
  {
    this.name = name;
    this.capabilities = capabilities;
    this.delegate = delegate;
  }

  // --- answered from the header, no fetch/build ---

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public ColumnCapabilities getCapabilities()
  {
    return capabilities;
  }

  @Override
  public ColumnMeta getMetaData()
  {
    // enough for asSignature/asSchema (valueType incl. typeName + multi-value); descs/stats need the real column
    return new ColumnMeta(capabilities.getTypeDesc(), capabilities.hasMultipleValues(), null, null);
  }
  // getType() (default) is served from getCapabilities() above — no build

  // --- realize the column (fetch + decode) ---

  @Override
  public int getNumRows()
  {
    return delegate.get().getNumRows();
  }

  @Override
  public boolean hasDictionaryEncodedColumn()
  {
    return delegate.get().hasDictionaryEncodedColumn();
  }

  @Override
  public boolean hasGenericColumn()
  {
    return delegate.get().hasGenericColumn();
  }

  @Override
  public boolean hasComplexColumn()
  {
    return delegate.get().hasComplexColumn();
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return delegate.get().compressionType();
  }

  @Override
  public Dictionary<String> getDictionary()
  {
    return delegate.get().getDictionary();
  }

  @Override
  public DictionaryEncodedColumn getDictionaryEncoded()
  {
    return delegate.get().getDictionaryEncoded();
  }

  @Override
  public RunLengthColumn getRunLengthColumn()
  {
    return delegate.get().getRunLengthColumn();
  }

  @Override
  public GenericColumn getGenericColumn()
  {
    return delegate.get().getGenericColumn();
  }

  @Override
  public ComplexColumn getComplexColumn()
  {
    return delegate.get().getComplexColumn();
  }

  @Override
  public BitmapIndex getBitmapIndex()
  {
    return delegate.get().getBitmapIndex();
  }

  @Override
  public SpatialIndex getSpatialIndex()
  {
    return delegate.get().getSpatialIndex();
  }

  @Override
  public HistogramBitmap getMetricBitmap()
  {
    return delegate.get().getMetricBitmap();
  }

  @Override
  public BitSlicedBitmap getBitSlicedBitmap()
  {
    return delegate.get().getBitSlicedBitmap();
  }

  @Override
  public Class<? extends GenericColumn> getGenericColumnType()
  {
    return delegate.get().getGenericColumnType();
  }

  @Override
  public Class<? extends ComplexColumn> getComplexColumnType()
  {
    return delegate.get().getComplexColumnType();
  }

  @Override
  public Set<Class> getExternalIndexKeys()
  {
    return delegate.get().getExternalIndexKeys();
  }

  @Override
  public <T> ExternalIndexProvider<T> getExternalIndex(Class<T> clazz)
  {
    return delegate.get().getExternalIndex(clazz);
  }

  @Override
  public ExternalIndexProvider<FSTHolder> getFST()
  {
    return delegate.get().getFST();
  }

  @Override
  public long getSerializedSize(EncodeType encodeType)
  {
    return delegate.get().getSerializedSize(encodeType);
  }

  @Override
  public Map<String, Object> getColumnStats()
  {
    return delegate.get().getColumnStats();
  }

  @Override
  public Map<String, String> getColumnDescs()
  {
    return delegate.get().getColumnDescs();
  }

  @Override
  public Column resolve(String expression)
  {
    return delegate.get().resolve(expression);
  }
}
