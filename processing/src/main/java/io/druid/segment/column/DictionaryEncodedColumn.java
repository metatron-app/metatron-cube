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

import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import org.roaringbitmap.IntIterator;

import java.io.Closeable;
import java.util.function.IntUnaryOperator;

/**
 */
public interface DictionaryEncodedColumn extends Closeable
{
  int length();
  boolean hasMultipleValues();
  int getSingleValueRow(int rowNum);
  int getSingleValueRow(int rowNum, int[] handover);
  IndexedInts getMultiValueRow(int rowNum);
  void scan(IntIterator iterator, IntScanner scanner);
  String lookupName(int id);
  int lookupId(String name);
  int getCardinality();

  IntIterator getSingleValueRows();
  Dictionary<String> dictionary();
  DictionaryEncodedColumn withDictionary(Dictionary<String> dictionary);

  default IntUnaryOperator asSupplier(int cache)
  {
    final int[] range = new int[]{-1, -1};
    final int[] cached = new int[cache];
    return x -> {
      if (x < range[0] || x >= range[1]) {
        final int valid = getSingleValueRow(x, cached);
        range[0] = x;
        range[1] = x + valid;
      }
      return cached[x  - range[0]];
    };
  }
}
