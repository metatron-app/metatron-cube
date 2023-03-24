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

import com.google.common.base.Strings;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public final class DictionaryEncodedColumn implements Closeable
{
  private final IndexedInts column;
  private final IndexedMultivalue<IndexedInts> multiValueColumn;
  private final Dictionary<String> dictionary;

  public DictionaryEncodedColumn(
      IndexedInts singleValueColumn,
      IndexedMultivalue<IndexedInts> multiValueColumn,
      Dictionary<String> dictionary
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.dictionary = dictionary;
  }

  public int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  public boolean hasMultipleValues()
  {
    return column == null;
  }

  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  public int getSingleValueRow(int rowNum, int[] handover)
  {
    return column.get(rowNum, handover);
  }

  public IntIterator getSingleValueRows()
  {
    return column.iterator();
  }

  public void scan(final IntIterator iterator, final IntScanner scanner)
  {
    column.scan(iterator, scanner);
  }

  private IndexedInts cached;
  private int index = -1;

  public IndexedInts getMultiValueRow(int rowNum)
  {
    return index == rowNum ? cached : (cached = multiValueColumn.get(index = rowNum));
  }

  public String lookupName(int id)
  {
    //Empty to Null will ensure that null and empty are equivalent for extraction function
    return Strings.emptyToNull(dictionary.get(id));
  }

  public int lookupId(String name)
  {
    return dictionary.indexOf(name);
  }

  public int cardinality()
  {
    return dictionary.size();
  }

  public Dictionary<String> dictionary()
  {
    return dictionary;
  }

  public DictionaryEncodedColumn withDictionary(Dictionary<String> dictionary)
  {
    return new DictionaryEncodedColumn(column, multiValueColumn, dictionary);
  }

  @Override
  public void close() throws IOException
  {
    if (column != null) {
      column.close();
    }
    if (multiValueColumn != null) {
      multiValueColumn.close();
    }
    if (dictionary != null) {
      dictionary.close();
    }
  }

  private static final int ID_CACHE_SIZE = 128;
  private static final float MINIMUM_SELECTIVITY = 0.33f;

  public static interface RowSuppler
  {
    int row(int x);
  }

  public static RowSuppler rowSupplier(DictionaryEncodedColumn column, float selectivity)
  {
    if (selectivity < MINIMUM_SELECTIVITY) {
      return column.column::get;
    }
    final IndexedInts source = IndexedInts.prepare(column.column, ID_CACHE_SIZE);
    final int[] range = new int[]{-1, -1};
    final int[] cached = new int[ID_CACHE_SIZE];
    return x -> {
      if (x < range[0] || x >= range[1]) {
        final int valid = source.get(x, cached);
        range[0] = x;
        range[1] = x + valid;
        return cached[0];
      }
      return cached[x - range[0]];
    };
  }
}
