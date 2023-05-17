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
import io.druid.common.utils.StringUtils;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.segment.bitmap.BitSets;
import io.druid.segment.column.IntScanner;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.IndexedInts;
import org.roaringbitmap.IntIterator;

import java.util.BitSet;
import java.util.function.IntFunction;

/**
 */
public interface DimensionSelector
{
  /**
   * Gets all values for the row inside an IntBuffer.  I.e. one possible implementation could be
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

    byte[] getAsRaw(int id);

    BufferRef getAsRef(int id);

    default UTF8Bytes getAsWrap(int id)
    {
      return UTF8Bytes.of(getAsRaw(id));
    }
  }

  // aka. dictionary with single value without extract function
  interface Scannable extends SingleValued, WithRawAccess
  {
    void scan(IntIterator iterator, IntScanner scanner);    // rowID to dictId

    void scan(Tools.Scanner scanner);         // on current

    <T> T apply(Tools.Function<T> function);  // on current

    // no duplication (if it's needed, use IntList)
    default void scan(IntIterator iterator, Tools.Scanner scanner)
    {
      getDictionary().scan(BitSets.iterator(collect(iterator)), scanner);
    }

    default BitSet collect(IntIterator iterator)
    {
      BitSet dictIds = new BitSet(getDictionary().size());
      scan(iterator, (x, v) -> dictIds.set(v.applyAsInt(x)));
      return dictIds;
    }
  }

  interface Mimic extends DimensionSelector, ObjectColumnSelector
  {
  }

  static IntFunction[] convert(DimensionSelector[] dimensions, ValueDesc[] dimensionTypes, boolean useRawUTF8)
  {
    final IntFunction[] rawAccess = new IntFunction[dimensions.length];
    for (int x = 0; x < rawAccess.length; x++) {
      final DimensionSelector selector = dimensions[x];
      if (useRawUTF8 && dimensionTypes[x].isStringOrDimension()) {
        rawAccess[x] = selector instanceof DimensionSelector.WithRawAccess ?
                       ix -> UTF8Bytes.of(((DimensionSelector.WithRawAccess) selector).getAsRaw(ix)) :
                       ix -> UTF8Bytes.of((String) selector.lookupName(ix));
      } else {
        rawAccess[x] = ix -> StringUtils.emptyToNull(selector.lookupName(ix));
      }
    }
    return rawAccess;
  }
}
