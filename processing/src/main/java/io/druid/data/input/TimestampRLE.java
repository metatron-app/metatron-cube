package io.druid.data.input;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class TimestampRLE implements Iterable<Long>
{
  private int size;
  private TimeWithCounter current;
  private final List<TimeWithCounter> entries = Lists.newArrayList();

  public TimestampRLE() {}

  public TimestampRLE(byte[] array)
  {
    final BytesInputStream bin = new BytesInputStream(array);
    size = bin.readShort();
    int numEntries = bin.readShort();
    for (int i = 0; i < numEntries; i++) {
      entries.add(new TimeWithCounter(bin.readLong(), bin.readShort()));
    }
  }

  public void add(long time)
  {
    if (current == null || current.timestamp != time) {
      entries.add(current = new TimeWithCounter(time, 1));
    } else {
      current.counter++;
    }
    size++;
  }

  public int size()
  {
    return size;
  }

  public byte[] flush()
  {
    final BytesOutputStream bout = new BytesOutputStream();
    bout.writeShort(size);
    bout.writeShort(entries.size());
    for (TimeWithCounter pair : entries) {
      bout.writeLong(pair.timestamp);
      bout.writeShort(pair.counter);
    }
    size = 0;
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
      private TimeWithCounter current = new TimeWithCounter(0, 0);
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
