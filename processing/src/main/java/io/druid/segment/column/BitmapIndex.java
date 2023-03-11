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
import io.druid.common.guava.BinaryRef;
import io.druid.data.UTF8Bytes;
import io.druid.query.filter.DimFilters;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;

import java.util.List;
import java.util.stream.IntStream;

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
  default int indexOf(String value)
  {
    return getDictionary().indexOf(value);
  }

  default IntStream indexOf(List<String> values)
  {
    return getDictionary().indexOf(values);
  }

  default IntStream indexOfRaw(List<BinaryRef> values)
  {
    return getDictionary().indexOfRaw(values);
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
