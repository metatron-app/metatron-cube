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

package io.druid.common;

import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import org.joda.time.Interval;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 */
public class KeyBuilder
{
  private static final ThreadLocal<BytesOutputStream> BUILDERS = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  public static KeyBuilder get(int limit)
  {
    return new KeyBuilder(BUILDERS.get(), limit);
  }

  private static final byte SEPARATOR = (byte) 0xff;

  private final BytesOutputStream output;
  private final int limit;
  private boolean disabled;

  private KeyBuilder(BytesOutputStream output, int limit)
  {
    this.output = output;
    this.limit = limit;
    output.clear();
  }

  public KeyBuilder append(byte value)
  {
    if (isValid()) {
      output.write(value);
    }
    return this;
  }

  public KeyBuilder append(byte[] value)
  {
    if (isValid()) {
      output.write(value);
    }
    return this;
  }

  public KeyBuilder append(boolean val)
  {
    if (isValid()) {
      output.writeBoolean(val);
    }
    return this;
  }

  public KeyBuilder append(boolean... vals)
  {
    if (isValid()) {
      for (boolean val : vals) {
        output.writeBoolean(val);
      }
    }
    return this;
  }

  public KeyBuilder append(int val)
  {
    if (isValid()) {
      output.writeInt(val);
    }
    return this;
  }

  public KeyBuilder append(int... vals)
  {
    for (int val : vals) {
      if (isValid()) {
        output.writeInt(val);
      }
    }
    return this;
  }

  public KeyBuilder append(float val)
  {
    if (isValid()) {
      output.writeFloat(val);
    }
    return this;
  }

  public KeyBuilder append(float... vals)
  {
    for (float val : vals) {
      if (isValid()) {
        output.writeFloat(val);
      }
    }
    return this;
  }

  public KeyBuilder append(double val)
  {
    if (isValid()) {
      output.writeDouble(val);
    }
    return this;
  }

  public KeyBuilder append(double... vals)
  {
    for (double val : vals) {
      if (isValid()) {
        output.writeDouble(val);
      }
    }
    return this;
  }

  public KeyBuilder append(Cacheable cacheable)
  {
    if (cacheable != null && isValid()) {
      return cacheable.getCacheKey(this);
    }
    return this;
  }

  public KeyBuilder append(List<? extends Cacheable> cacheables)
  {
    if (!GuavaUtils.isNullOrEmpty(cacheables)) {
      Iterator<? extends Cacheable> iterator = cacheables.iterator();
      while (iterator.hasNext() && isValid()) {
        iterator.next().getCacheKey(sp());
      }
    }
    return this;
  }

  public KeyBuilder appendAll(List<List<String>> strings)
  {
    if (strings == null || !isValid()) {
      return this;
    }
    Iterator<List<String>> iterator = strings.iterator();
    if (!iterator.hasNext()) {
      return this;
    }
    while (iterator.hasNext() && isValid()) {
      append(iterator.next());
    }
    return this;
  }

  public KeyBuilder append(Iterable<String> strings)
  {
    if (strings == null || !isValid()) {
      return this;
    }
    Iterator<String> iterator = strings.iterator();
    if (!iterator.hasNext()) {
      return this;
    }
    output.write(StringUtils.toUtf8WithNullToEmpty(iterator.next()));
    while (iterator.hasNext() && isValid()) {
      output.write(SEPARATOR);
      output.write(StringUtils.toUtf8WithNullToEmpty(iterator.next()));
    }
    return this;
  }

  public KeyBuilder append(String string)
  {
    if (string != null && isValid()) {
      output.write(StringUtils.toUtf8(string));
    }
    return this;
  }

  public KeyBuilder append(String first, String... remaining)
  {
    if (!isValid()) {
      return this;
    }
    output.write(StringUtils.toUtf8WithNullToEmpty(first));
    for (String string : remaining) {
      if (!isValid()) {
        break;
      }
      output.write(SEPARATOR);
      output.write(StringUtils.toUtf8WithNullToEmpty(string));
    }
    return this;
  }

  public KeyBuilder append(Enum value)
  {
    if (isValid()) {
      output.writeByte(value.ordinal());
    }
    return this;
  }

  public KeyBuilder append(EnumSet<?> values)
  {
    for (Enum value : values) {
      if (isValid()) {
        output.writeByte(value.ordinal());
      }
    }
    return this;
  }

  public KeyBuilder append(Object value)
  {
    return isValid() ? append(Objects.toString(value, null)) : this;
  }

  public KeyBuilder appendIntervals(List<Interval> intervals)
  {
    for (Interval interval : intervals) {
      if (isValid()) {
        appendInterval(interval);
      }
    }
    return this;
  }

  public KeyBuilder appendInterval(Interval interval)
  {
    if (isValid()) {
      output.writeLong(interval.getStartMillis());
      output.writeLong(interval.getEndMillis());
    }
    return this;
  }

  public KeyBuilder sp()
  {
    if (isValid()) {
      output.write(SEPARATOR);
    }
    return this;
  }

  public int available()
  {
    return limit - output.size();
  }

  public void disable()
  {
    disabled = true;
  }

  public byte[] build()
  {
    return isValid() ? output.toByteArray() : null;
  }

  private boolean isValid()
  {
    return !disabled && output.size() < limit;
  }
}
