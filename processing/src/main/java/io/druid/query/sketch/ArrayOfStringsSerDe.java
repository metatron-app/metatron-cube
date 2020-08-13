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

package io.druid.query.sketch;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import io.druid.common.utils.StringUtils;
import io.druid.data.UTF8Bytes;

public class ArrayOfStringsSerDe extends ArrayOfItemsSerDe
{
  @Override
  public byte[] serializeToByteArray(final Object[] items)
  {
    int length = 0;
    final byte[][] itemsBytes = new byte[items.length][];
    for (int i = 0; i < items.length; i++) {
      if (items[i] instanceof UTF8Bytes) {
        itemsBytes[i] = ((UTF8Bytes) items[i]).getValue();
      } else {
        itemsBytes[i] = StringUtils.toUtf8((String) items[i]);
      }
      length += itemsBytes[i].length + Integer.BYTES;
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
    long offsetBytes = 0;
    for (int i = 0; i < items.length; i++) {
      mem.putInt(offsetBytes, itemsBytes[i].length);
      offsetBytes += Integer.BYTES;
      mem.putByteArray(offsetBytes, itemsBytes[i], 0, itemsBytes[i].length);
      offsetBytes += itemsBytes[i].length;
    }
    return bytes;
  }

  @Override
  public String[] deserializeFromMemory(final Memory mem, final int numItems)
  {
    final String[] array = new String[numItems];
    long offsetBytes = 0;
    for (int i = 0; i < numItems; i++) {
      UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      final int strLength = mem.getInt(offsetBytes);
      offsetBytes += Integer.BYTES;
      final byte[] bytes = new byte[strLength];
      UnsafeUtil.checkBounds(offsetBytes, strLength, mem.getCapacity());
      mem.getByteArray(offsetBytes, bytes, 0, strLength);
      offsetBytes += strLength;
      array[i] = StringUtils.fromUtf8(bytes);
    }
    return array;
  }
}
