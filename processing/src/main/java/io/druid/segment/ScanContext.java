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

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.bitmap.Bitmaps;
import org.roaringbitmap.IntIterator;

import java.util.Map;

public class ScanContext
{
  private final Scanning scanning;
  private final ImmutableBitmap bitmap;
  private final Map<String, ImmutableBitmap> ranges;
  private final int[] range;
  private final int numRows;

  public ScanContext(
      Scanning scanning,
      ImmutableBitmap bitmap,
      Map<String, ImmutableBitmap> ranges,
      int[] range,
      int numRows
  )
  {
    this.scanning = scanning;
    this.bitmap = bitmap;
    this.ranges = ranges;
    this.range = range;
    this.numRows = numRows;
  }

  public Scanning scanning()
  {
    return scanning;
  }

  public ImmutableBitmap dictionaryRef(String dimension)
  {
    return ranges == null ? null : ranges.get(dimension);
  }

  public int numRows()
  {
    return numRows;
  }

  public int count()
  {
    return Bitmaps.count(bitmap, range);
  }

  public IntIterator iterator()
  {
    return Bitmaps.filter(bitmap, range);
  }

  public boolean is(Scanning scanning)
  {
    return this.scanning == scanning;
  }

  public boolean awareTargetRows()
  {
    return scanning.awareTargetRows();
  }
}
