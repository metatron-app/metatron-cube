/*
 * Copyright 2011 - 2015 SK Telecom Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.roaringbitmap.buffer;

import io.druid.data.input.BytesOutputStream;
import io.druid.segment.bitmap.ExtendedBitmap;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.bitmap.WrappedImmutableRoaringBitmap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RoaringUtilsTest
{
  @Test
  public void testCardinality() throws IOException
  {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(0, 1, 3, 5, 9, 0xffff);
    bitmap.add(0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5, 0x10000 + 9, 0x10000 + 0xffff);
    test(new WrappedImmutableRoaringBitmap(bitmap));

    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    BytesOutputStream out = new BytesOutputStream();
    bitmap.serialize(out);
    ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
    test((ExtendedBitmap) factory.mapImmutableBitmap(buffer, buffer.position(), buffer.remaining()));
  }

  private void test(ExtendedBitmap bitmap)
  {
    _test(bitmap, new int[]{0, 0xffff}, 0, 1, 3, 5, 9, 0xffff);
    _test(bitmap, new int[]{0x10000, 0x10000 + 0xffff}, 0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5, 0x10000 + 9, 0x10000 + 0xffff);
    _test(bitmap, new int[]{0, bitmap.last()}, 0, 1, 3, 5, 9, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5, 0x10000 + 9, 0x10000 + 0xffff);

    _test(bitmap, new int[]{0x100, 0x10000}, 0xffff, 0x10000);
    _test(bitmap, new int[]{0x100, 0x10000 + 1}, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{0x100, 0x10000 + 2}, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{0x100, 0x10000 + 3}, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3);
    _test(bitmap, new int[]{0x100, 0x10000 + 4}, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3);
    _test(bitmap, new int[]{0x100, 0x10000 + 5}, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5);
    _test(bitmap, new int[]{0x100, 0x10000 + 9}, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5, 0x10000 + 9);
    _test(bitmap, new int[]{0x100, 0x10000 + 0xffff}, 0xffff, 0x10000, 0x10000 + 1, 0x10000 + 3, 0x10000 + 5, 0x10000 + 9, 0x10000 + 0xffff);

    _test(bitmap, new int[]{1, 0x10000 + 2}, 1, 3, 5, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{2, 0x10000 + 2}, 3, 5, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{3, 0x10000 + 2}, 3, 5, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{4, 0x10000 + 2}, 5, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{5, 0x10000 + 2}, 5, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{9, 0x10000 + 2}, 9, 0xffff, 0x10000, 0x10000 + 1);
    _test(bitmap, new int[]{0xffff, 0x10000 + 2}, 0xffff, 0x10000, 0x10000 + 1);
  }

  private void _test(ExtendedBitmap bitmap, int[] range, int... expected)
  {
    Assert.assertEquals(expected.length, bitmap.cardinality(range));
    Assert.assertArrayEquals(expected, IntIterators.toArray(bitmap.iterator(range)));
  }
}
