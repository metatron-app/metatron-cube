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
import org.apache.lucene.util.fst.FST;
import org.roaringbitmap.IntIterator;

import java.io.IOException;

/**
 */
public class SimpleDictionaryEncodedColumn implements DictionaryEncodedColumn
{
  private final IndexedInts column;
  private final IndexedMultivalue<IndexedInts> multiValueColumn;
  private final Dictionary<String> dictionary;
  private final FST<Long> fst;

  public SimpleDictionaryEncodedColumn(
      IndexedInts singleValueColumn,
      IndexedMultivalue<IndexedInts> multiValueColumn,
      Dictionary<String> dictionary,
      FST<Long> fst
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.dictionary = dictionary;
    this.fst = fst;
  }

  @Override
  public int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return column == null;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  public void scan(final IntIterator iterator, final IntScanner scanner)
  {
    column.scan(iterator, scanner);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    return multiValueColumn.get(rowNum);
  }

  @Override
  public String lookupName(int id)
  {
    //Empty to Null will ensure that null and empty are equivalent for extraction function
    return Strings.emptyToNull(dictionary.get(id));
  }

  @Override
  public int lookupId(String name)
  {
    return dictionary.indexOf(name);
  }

  @Override
  public int getCardinality()
  {
    return dictionary.size();
  }

  @Override
  public Dictionary<String> dictionary()
  {
    return dictionary;
  }

  @Override
  public FST<Long> getFST()
  {
    return fst;
  }

  @Override
  public DictionaryEncodedColumn withDictionary(Dictionary<String> dictionary)
  {
    return new SimpleDictionaryEncodedColumn(column, multiValueColumn, dictionary, fst);
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
}
