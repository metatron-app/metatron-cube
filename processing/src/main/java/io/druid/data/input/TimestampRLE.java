/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class TimestampRLE implements Iterable<Long>
{
  private TimeWithCounter current;
  private final List<TimeWithCounter> entries;

  public TimestampRLE()
  {
    entries = Lists.newArrayList();
  }

  public TimestampRLE(byte[] array)
  {
    final BytesInputStream bin = new BytesInputStream(array);
    final int numEntries = bin.readUnsignedVarInt();
    entries = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      TimeWithCounter entry = new TimeWithCounter(bin.readVarLong(), bin.readUnsignedVarInt());
      entries.add(entry);
    }
  }

  public void add(long time)
  {
    if (current == null || current.timestamp != time) {
      entries.add(current = new TimeWithCounter(time, 1));
    } else {
      current.counter++;
    }
  }

  public byte[] flush()
  {
    final BytesOutputStream bout = new BytesOutputStream();
    bout.writeUnsignedVarInt(entries.size());
    for (TimeWithCounter pair : entries) {
      bout.writeVarLong(pair.timestamp);
      bout.writeUnsignedVarInt(pair.counter);
    }
    current = null;
    entries.clear();
    return bout.toByteArray();
  }

  @Override
  public Iterator<Long> iterator()
  {
    return new Iterator<Long>()
    {
      private final Iterator<TimeWithCounter> pairs = entries.iterator();
      private TimeWithCounter current = new TimeWithCounter(-1, -1);
      private int index;

      @Override
      public boolean hasNext()
      {
        for (; index >= current.counter && pairs.hasNext(); current = pairs.next(), index = 0) {
        }
        return index < current.counter;
      }

      @Override
      public Long next()
      {
        index++;
        return current.timestamp;
      }
    };
  }

  private static class TimeWithCounter
  {
    private final long timestamp;
    private int counter;

    private TimeWithCounter(long timestamp, int counter)
    {
      this.timestamp = timestamp;
      this.counter = counter;
    }
  }
}
