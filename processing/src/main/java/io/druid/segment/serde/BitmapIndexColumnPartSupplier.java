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

package io.druid.segment.serde;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;

/**
 */
public class BitmapIndexColumnPartSupplier implements ColumnPartProvider<BitmapIndex>
{
  private final BitmapFactory bitmapFactory;
  private final GenericIndexed<ImmutableBitmap> bitmaps;
  private final ColumnPartProvider<Dictionary<String>> provider;

  private final int[] cumulativeThresholds;
  private final GenericIndexed<ImmutableBitmap> cumulativeBitmaps;

  public BitmapIndexColumnPartSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ColumnPartProvider<Dictionary<String>> provider
  )
  {
    this(bitmapFactory, bitmaps, provider, null, null);
  }

  public BitmapIndexColumnPartSupplier(
      BitmapFactory bitmapFactory,
      GenericIndexed<ImmutableBitmap> bitmaps,
      ColumnPartProvider<Dictionary<String>> provider,
      int[] cumulativeThresholds,
      GenericIndexed<ImmutableBitmap> cumulativeBitmaps
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.bitmaps = bitmaps;
    this.provider = provider;
    this.cumulativeThresholds = cumulativeThresholds;
    this.cumulativeBitmaps = cumulativeBitmaps;
  }

  @Override
  public int numRows()
  {
    return bitmaps.size();
  }

  @Override
  public long getSerializedSize()
  {
    return bitmaps.getSerializedSize();
  }

  @Override
  public Class<? extends BitmapIndex> provides()
  {
    return BitmapIndex.CumulativeSupport.class;
  }

  @Override
  public BitmapIndex get()
  {
    final GenericIndexed<ImmutableBitmap> dedicated = bitmaps.asSingleThreaded();
    final Dictionary<String> dictionary = provider == null ? null : provider.get();
    return new BitmapIndex.CumulativeSupport()
    {
      @Override
      public Dictionary<String> getDictionary()
      {
        return dictionary;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return bitmapFactory;
      }

      @Override
      public ImmutableBitmap getBitmap(int idx)
      {
        return getImmutableBitmap(dedicated, idx);
      }

      @Override
      public GenericIndexed<ImmutableBitmap> getBitmaps()
      {
        return dedicated;
      }

      private ImmutableBitmap getImmutableBitmap(GenericIndexed<ImmutableBitmap> bitmaps, int idx)
      {
        if (idx < 0) {
          return bitmapFactory.makeEmptyImmutableBitmap();
        }
        final ImmutableBitmap bitmap = bitmaps.get(idx);
        return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
      }

      @Override
      public int[] thresholds()
      {
        return cumulativeThresholds;
      }

      @Override
      public ImmutableBitmap getCumulative(int idx)
      {
        return getImmutableBitmap(cumulativeBitmaps, idx);
      }

      @Override
      public GenericIndexed<ImmutableBitmap> getCumulativeBitmaps()
      {
        return cumulativeBitmaps == null ? null : cumulativeBitmaps.asSingleThreaded();
      }
    };
  }
}
