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

package io.druid.query.aggregation.distinctcount;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.segment.bitmap.RoaringBitmapFactory;

public class RoaringBitMapFactory implements BitMapFactory
{
  private static final BitmapFactory bitmapFactory = new RoaringBitmapFactory();

  public RoaringBitMapFactory() {}

  @Override
  public MutableBitmap makeEmptyMutableBitmap()
  {
    return bitmapFactory.makeEmptyMutableBitmap();
  }

  @Override
  public String toString()
  {
    return "RoaringBitMapFactory";
  }

  @Override
  public boolean equals(Object o)
  {
    return this == o || o instanceof RoaringBitMapFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append((byte) 0x02);
  }
}
