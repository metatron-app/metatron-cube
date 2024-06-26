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

package io.druid.segment.data;

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.segment.bitmap.RoaringBitmapFactory;

import java.nio.ByteBuffer;

/**
 */
public class RoaringBitmapSerdeFactory implements BitmapSerdeFactory
{
  public static final ObjectStrategy<ImmutableBitmap> objectStrategy = new ImmutableRoaringBitmapObjectStrategy();
  public static final RoaringBitmapFactory bitmapFactory = new RoaringBitmapFactory();
  public static final RoaringBitmapFactory bitmapFactoryOptimized = new RoaringBitmapFactory(true);

  @Override
  public ObjectStrategy<ImmutableBitmap> getObjectStrategy()
  {
    return objectStrategy;
  }

  @Override
  public BitmapFactory getBitmapFactory()
  {
    return bitmapFactory;
  }

  @Override
  public BitmapFactory getBitmapFactory(boolean optimizeForSerialization)
  {
    return optimizeForSerialization ? bitmapFactory : bitmapFactoryOptimized;
  }

  private static class ImmutableRoaringBitmapObjectStrategy
      implements ObjectStrategy<ImmutableBitmap>
  {
    @Override
    public Class<ImmutableBitmap> getClazz()
    {
      return ImmutableBitmap.class;
    }

    @Override
    public ImmutableBitmap fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      if (buffer.remaining() == 0) {
        return bitmapFactory.makeEmptyImmutableBitmap();
      }
      return bitmapFactory.mapImmutableBitmap(buffer, buffer.position(), numBytes);
    }

    @Override
    public byte[] toBytes(ImmutableBitmap val)
    {
      if (val == null || val.isEmpty()) {
        return new byte[]{};
      }
      return val.toBytes();
    }
  }

  @Override
  public String toString()
  {
    return "RoaringBitmapSerdeFactory{}";
  }

  @Override
  public boolean equals(Object o)
  {
    return o instanceof RoaringBitmapSerdeFactory;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}
