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

package io.druid.segment.serde;

import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IntValues;
import io.druid.segment.data.IntsValues;

/**
*/
public class DictionaryEncodedColumnSupplier implements ColumnPartProvider.DictionarySupport
{
  public static class Builder
  {
    public ColumnPartProvider<Dictionary<String>> dictionary;
    public ColumnPartProvider<IntValues> singleValuedColumn;
    public ColumnPartProvider<IntsValues> multiValuedColumn;

    public DictionaryEncodedColumnSupplier build()
    {
      return dictionary == null ? null : new DictionaryEncodedColumnSupplier(dictionary, singleValuedColumn, multiValuedColumn);
    }
  }

  private final ColumnPartProvider<Dictionary<String>> dictionary;
  private final ColumnPartProvider<IntValues> singleValuedColumn;
  private final ColumnPartProvider<IntsValues> multiValuedColumn;

  private DictionaryEncodedColumnSupplier(
      ColumnPartProvider<Dictionary<String>> dictionary,
      ColumnPartProvider<IntValues> singleValuedColumn,
      ColumnPartProvider<IntsValues> multiValuedColumn
  )
  {
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
  }

  @Override
  public Dictionary<String> getDictionary()
  {
    return dictionary != null ? dictionary.get() : null;
  }

  @Override
  public int numRows()
  {
    return singleValuedColumn != null ? singleValuedColumn.numRows() : multiValuedColumn.numRows();
  }

  @Override
  public long getSerializedSize()
  {
    return 5 +
           (dictionary == null ? 0 : dictionary.getSerializedSize()) +
           (singleValuedColumn != null ? singleValuedColumn.getSerializedSize() : multiValuedColumn.getSerializedSize());
  }

  @Override
  public Class<? extends DictionaryEncodedColumn> provides()
  {
    return DictionaryEncodedColumn.class;
  }

  @Override
  public DictionaryEncodedColumn get()
  {
    return new DictionaryEncodedColumn(
        singleValuedColumn != null ? singleValuedColumn.get() : null,
        multiValuedColumn != null ? multiValuedColumn.get() : null,
        dictionary != null ? dictionary.get() : null
    );
  }
}
