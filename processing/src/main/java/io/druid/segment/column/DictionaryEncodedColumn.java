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
import io.druid.segment.data.IntsValues;
import io.druid.segment.filter.FilterContext;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

/**
 *
 */
public final class DictionaryEncodedColumn implements Closeable
{
  private final IndexedInts column;
  private final IntsValues multiValueColumn;
  private final Dictionary<String> dictionary;

  public DictionaryEncodedColumn(
      IndexedInts singleValueColumn,
      IntsValues multiValueColumn,
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

  public Object getSingleValued(int rowNum)
  {
    return lookupName(getSingleValueRow(rowNum));
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
    if (column != null) {
      column.scan(iterator, scanner);
    } else {
      multiValueColumn.scan(iterator, scanner); // not sure
    }
  }

  public void consume(final IntIterator iterator, final IntIntConsumer consumer)
  {
    if (column != null) {
      column.consume(iterator, consumer);
    } else {
      multiValueColumn.consume(iterator, consumer); // not sure
    }
  }

  // collects dictionary IDs
  public BitSet collect(final IntIterator iterator)
  {
    BitSet set = new BitSet();
    scan(iterator, (x, v) -> set.set(v.applyAsInt(x)));
    return set;
  }

  private IndexedInts cached;
  private int index = -1;

  public IndexedInts getMultiValueRow(int rowNum)
  {
    return index == rowNum ? cached : (cached = multiValueColumn.get(index = rowNum));
  }

  public Object getMultiValued(int rowNum)
  {
    final IndexedInts indexedInts = getMultiValueRow(rowNum);
    final int length = indexedInts.size();
    if (length == 0) {
      return null;
    } else if (length == 1) {
      return lookupName(indexedInts.get(0));
    } else {
      final String[] strings = new String[length];
      for (int i = 0; i < length; i++) {
        strings[i] = lookupName(indexedInts.get(i));
      }
      return Arrays.asList(strings);
    }
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
  private static final float MINIMUM_SELECTIVITY = 0.25f;

  public RowSupplier row(FilterContext context)
  {
    if (context == null || context.selectivity() < MINIMUM_SELECTIVITY) {
      return x -> column.get(x);
    }
    final IndexedInts source = IndexedInts.prepare(column, ID_CACHE_SIZE);
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
