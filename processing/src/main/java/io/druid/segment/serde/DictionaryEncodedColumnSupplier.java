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
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import org.apache.lucene.util.fst.FST;

/**
*/
public class DictionaryEncodedColumnSupplier implements ColumnPartProvider.DictionarySupport
{
  private final ColumnPartProvider<Dictionary<String>> dictionary;
  private final ColumnPartProvider<FST<Long>> fst;
  private final ColumnPartProvider<IndexedInts> singleValuedColumn;
  private final ColumnPartProvider<IndexedMultivalue<IndexedInts>> multiValuedColumn;

  public DictionaryEncodedColumnSupplier(
      ColumnPartProvider<Dictionary<String>> dictionary,
      ColumnPartProvider<FST<Long>> fst,
      ColumnPartProvider<IndexedInts> singleValuedColumn,
      ColumnPartProvider<IndexedMultivalue<IndexedInts>> multiValuedColumn
  )
  {
    this.dictionary = dictionary;
    this.fst = fst;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
  }

  @Override
  public Dictionary<String> getDictionary()
  {
    return dictionary == null ? null : dictionary.get();
  }

  @Override
  public DictionaryEncodedColumn get()
  {
    return new SimpleDictionaryEncodedColumn(
        singleValuedColumn != null ? singleValuedColumn.get() : null,
        multiValuedColumn != null ? multiValuedColumn.get() : null,
        dictionary == null ? null : dictionary.get(),
        fst == null ? null : fst.get()
    );
  }

  @Override
  public int numRows()
  {
    return singleValuedColumn != null ? singleValuedColumn.numRows() : multiValuedColumn.numRows();
  }

  @Override
  public long getSerializedSize()
  {
    return (dictionary == null ? 0 : dictionary.getSerializedSize()) +
           (fst == null ? 0 : fst.getSerializedSize()) +
           (singleValuedColumn != null ? singleValuedColumn.getSerializedSize() : multiValuedColumn.getSerializedSize());
  }
}
