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

package io.druid.segment.filter;

import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.Pair;

import java.util.List;

public class BitmapHolder extends Pair<Boolean, ImmutableBitmap>
{
  public static BitmapHolder of(boolean exact, ImmutableBitmap bitmap)
  {
    return bitmap == null ? null : new BitmapHolder(exact, bitmap);
  }

  public static BitmapHolder exact(ImmutableBitmap bitmap)
  {
    return bitmap == null ? null : new BitmapHolder(true, bitmap);
  }

  public static BitmapHolder notExact(ImmutableBitmap bitmap)
  {
    return bitmap == null ? null : new BitmapHolder(false, bitmap);
  }

  private BitmapHolder(boolean lhs, ImmutableBitmap rhs)
  {
    super(lhs, rhs);
  }

  public boolean exact()
  {
    return lhs;
  }

  public ImmutableBitmap bitmap()
  {
    return rhs;
  }

  public int size()
  {
    return rhs.size();
  }

  public static BitmapHolder union(FilterContext context, List<BitmapHolder> holders)
  {
    if (GuavaUtils.isNullOrEmpty(holders)) {
      return null;
    }
    if (holders.size() == 1) {
      return holders.get(0);
    }
    boolean exact = true;
    List<ImmutableBitmap> bitmaps = Lists.newArrayList();
    for (BitmapHolder holder : holders) {
      if (holder == null) {
        continue;
      }
      ImmutableBitmap bitmap = holder.bitmap();
      if (bitmap.isEmpty()) {
        continue;
      }
      if (context.isAll(bitmap)) {
        return BitmapHolder.of(exact & holder.exact(), bitmap);
      }
      exact &= holder.exact();
      bitmaps.add(bitmap);
    }
    if (bitmaps.isEmpty()) {
      return null;
    } else if (bitmaps.size() == 1) {
      return BitmapHolder.of(exact, bitmaps.get(0));
    } else {
      return of(exact, context.factory.union(bitmaps));
    }
  }

  public static BitmapHolder intersection(FilterContext context, List<BitmapHolder> holders)
  {
    return intersection(context, holders, true);
  }

  public static BitmapHolder intersection(FilterContext context, List<BitmapHolder> holders, boolean exact)
  {
    if (GuavaUtils.isNullOrEmpty(holders)) {
      return null;
    }
    if (holders.size() == 1) {
      BitmapHolder holder = holders.get(0);
      return exact ^ holder.exact() ? BitmapHolder.of(exact, holder.rhs) : holder;
    }
    List<ImmutableBitmap> bitmaps = Lists.newArrayList();
    for (BitmapHolder holder : holders) {
      if (holder == null) {
        continue;
      }
      ImmutableBitmap bitmap = holder.bitmap();
      if (context.isAll(bitmap)) {
        continue;
      }
      if (bitmap.isEmpty()) {
        return BitmapHolder.of(exact & holder.exact(), bitmap);
      }
      exact &= holder.exact();
      bitmaps.add(bitmap);
    }
    if (bitmaps.isEmpty()) {
      return null;
    } else if (bitmaps.size() == 1) {
      return BitmapHolder.of(exact, bitmaps.get(0));
    } else {
      return of(exact, context.factory.intersection(bitmaps));
    }
  }

  public static BitmapHolder not(BitmapFactory factory, BitmapHolder holder, int size)
  {
    return holder == null ? null : holder.exact() ? exact(factory.complement(holder.bitmap(), size)) : null;
  }
}
