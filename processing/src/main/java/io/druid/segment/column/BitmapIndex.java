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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.collections.IntList;
import io.druid.query.filter.DimFilters;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;

/**
 */
public interface BitmapIndex
{
  Dictionary<String> getDictionary();

  default int getCardinality()
  {
    return getDictionary().size();
  }

  default String getValue(int index)
  {
    return getDictionary().get(index);
  }

  default byte[] getValueAsRaw(int index)
  {
    return getDictionary().getAsRaw(index);
  }

  /**
   * Returns the index of "value" in this BitmapIndex, or (-(insertion point) - 1) if the value is not
   * present, in the manner of Arrays.binarySearch.
   *
   * @param value value to search for
   * @return index of value, or negative number equal to (-(insertion point) - 1).
   */
  default int getIndex(String value)
  {
    return getIndex(value, 0, true);
  }

  default int getIndex(String value, int start, boolean binary)
  {
    // GenericIndexed.indexOf satisfies contract needed by BitmapIndex.indexOf
    return getDictionary().indexOf(value, start, binary);
  }

  BitmapFactory getBitmapFactory();

  ImmutableBitmap getBitmap(int idx);

  GenericIndexed<ImmutableBitmap> getBitmaps();

  default ImmutableBitmap union(IntList indices)
  {
    return DimFilters.union(getBitmapFactory(), indices.transform(x -> getBitmap(x)));
  }

  interface CumulativeSupport extends BitmapIndex
  {
    int[] thresholds();

    ImmutableBitmap getCumulative(int idx);

    GenericIndexed<ImmutableBitmap> getCumulativeBitmaps();
  }
}
