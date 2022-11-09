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

package io.druid.segment;

import io.druid.common.guava.BufferRef;
import io.druid.data.ValueDesc;
import io.druid.segment.column.IntScanner;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import org.roaringbitmap.IntIterator;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside of an IntBuffer.  I.e. one possible implementation could be
   *
   * return IntBuffer.wrap(lookupExpansion(get());
   *
   * @return all values for the row as an IntBuffer
   */
  IndexedInts getRow();

  /**
   * Value cardinality is the cardinality of the different occurring values.  If there were 4 rows:
   *
   * A,B
   * A
   * B
   * A
   *
   * Value cardinality would be 2.
   *
   * @return the value cardinality
   */
  int getValueCardinality();

  /**
   * The Name is the String name of the actual field.  It is assumed that storage layers convert names
   * into id values which can then be used to get the string value.  For example
   *
   * A,B
   * A
   * A,B
   * B
   *
   * getRow() would return
   *
   * getRow(0) =&gt; [0 1]
   * getRow(1) =&gt; [0]
   * getRow(2) =&gt; [0 1]
   * getRow(3) =&gt; [1]
   *
   * and then lookupName would return:
   *
   * lookupName(0) =&gt; A
   * lookupName(1) =&gt; B
   *
   * @param id id to lookup the field name for
   * @return the field name for the given id
   */
  Object lookupName(int id);

  /**
   * returns without dimension or multivalue prefix
   *
   * @return type
   */
  ValueDesc type();

  /**
   * The ID is the int id value of the field.
   *
   * @param name field name to look up the id for
   * @return the id for the given field name
   */
  int lookupId(Object name);

  default boolean withSortedDictionary()
  {
    return false;
  }

  interface SingleValued extends DimensionSelector
  {
  }

  // aka. dictionary without extract function
  interface WithRawAccess extends DimensionSelector
  {
    Dictionary getDictionary();

    byte[] lookupRaw(int id);

    BufferRef getAsRef(int id);
  }

  // aka. dictionary with single value without extract function
  interface Scannable extends SingleValued, WithRawAccess
  {
    void scan(IntIterator iterator, IntScanner scanner);    // rowID to dictId

    void scan(Tools.Scanner scanner);         // on current

    <T> T apply(Tools.Function<T> function);  // on current
  }
}
