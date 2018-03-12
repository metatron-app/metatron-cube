/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.sketch;

import com.google.common.primitives.Floats;
import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;

/**
 */
public class ArrayOfFloatsSerDe extends ArrayOfItemsSerDe<Float>
{
  @Override
  public byte[] serializeToByteArray(final Float[] items) {
    final byte[] bytes = new byte[Floats.BYTES * items.length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putFloat(offsetBytes, items[i]);
      offsetBytes += Floats.BYTES;
    }
    return bytes;
  }

  @Override
  public Float[] deserializeFromMemory(final Memory mem, final int length) {
    UnsafeUtil.checkBounds(0, Floats.BYTES, mem.getCapacity());
    final Float[] array = new Float[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      array[i] = mem.getFloat(offsetBytes);
      offsetBytes += Floats.BYTES;
    }
    return array;
  }

}
