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
    final int numEntries = bin.readUnsignedShort();
    entries = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      TimeWithCounter entry = new TimeWithCounter(bin.readLong(), bin.readShort());
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
    bout.writeShort(entries.size());
    for (TimeWithCounter pair : entries) {
      bout.writeLong(pair.timestamp);
      bout.writeShort(pair.counter);
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
