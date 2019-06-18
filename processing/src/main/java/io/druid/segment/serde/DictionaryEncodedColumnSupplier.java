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
import io.druid.segment.data.DictionarySketch;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;

/**
*/
public class DictionaryEncodedColumnSupplier implements ColumnPartProvider.DictionarySupport
{
  private final GenericIndexed<String> dictionary;
  private final ColumnPartProvider<IndexedInts> singleValuedColumn;
  private final ColumnPartProvider<IndexedMultivalue<IndexedInts>> multiValuedColumn;
  private final DictionarySketch sketch;

  public DictionaryEncodedColumnSupplier(
      GenericIndexed<String> dictionary,
      ColumnPartProvider<IndexedInts> singleValuedColumn,
      ColumnPartProvider<IndexedMultivalue<IndexedInts>> multiValuedColumn,
      DictionarySketch sketch
  )
  {
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.sketch = sketch;
  }

  @Override
  public DictionaryEncodedColumn get()
  {
    return new SimpleDictionaryEncodedColumn(
        singleValuedColumn != null ? singleValuedColumn.get() : null,
        multiValuedColumn != null ? multiValuedColumn.get() : null,
        dictionary.asSingleThreaded(),
        sketch
    );
  }

  @Override
  public int size()
  {
    return singleValuedColumn != null ? singleValuedColumn.size() : multiValuedColumn.size();
  }

  @Override
  public long getSerializedSize()
  {
    return dictionary.getSerializedSize() +
           (singleValuedColumn != null ? singleValuedColumn.getSerializedSize() : multiValuedColumn.getSerializedSize());
  }

  @Override
  public GenericIndexed<String> getDictionary()
  {
    return dictionary;
  }
}
