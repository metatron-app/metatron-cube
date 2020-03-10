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
  private static ThreadLocal<BytesOutputStream> BUILDERS = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  public static KeyBuilder get()
  {
    return new KeyBuilder(BUILDERS.get());
  }

  private static final byte SEPARATOR = (byte) 0xff;

  private final BytesOutputStream output;

  private KeyBuilder(BytesOutputStream output)
  {
    this.output = output;
    output.clear();
  }

  public KeyBuilder append(byte value)
  {
    output.write(value);
    return this;
  }

  public KeyBuilder append(byte[] value)
  {
    output.write(value);
    return this;
  }

  public KeyBuilder append(boolean val)
  {
    output.writeBoolean(val);
    return this;
  }

  public KeyBuilder append(boolean... vals)
  {
    for (boolean val : vals) {
      output.writeBoolean(val);
    }
    return this;
  }

  public KeyBuilder append(int val)
  {
    output.writeInt(val);
    return this;
  }

  public KeyBuilder append(int... vals)
  {
    for (int val : vals) {
      output.writeInt(val);
    }
    return this;
  }

  public KeyBuilder append(float val)
  {
    output.writeFloat(val);
    return this;
  }

  public KeyBuilder append(float... vals)
  {
    for (float val : vals) {
      output.writeFloat(val);
    }
    return this;
  }

  public KeyBuilder append(double val)
  {
    output.writeDouble(val);
    return this;
  }

  public KeyBuilder append(double... vals)
  {
    for (double val : vals) {
      output.writeDouble(val);
    }
    return this;
  }

  public KeyBuilder append(Cacheable cacheable)
  {
    if (cacheable != null) {
      return cacheable.getCacheKey(this);
    }
    return this;
  }

  public KeyBuilder append(List<? extends Cacheable> cacheables)
  {
    if (cacheables != null) {
      for (Cacheable cacheable : cacheables) {
        cacheable.getCacheKey(sp());
      }
    }
    return this;
  }

  public KeyBuilder append(Iterable<String> strings)
  {
    if (strings == null) {
      return this;
    }
    Iterator<String> iterator = strings.iterator();
    if (!iterator.hasNext()) {
      return this;
    }
    output.write(StringUtils.toUtf8WithNullToEmpty(iterator.next()));
    while (iterator.hasNext()) {
      output.write(SEPARATOR);
      output.write(StringUtils.toUtf8WithNullToEmpty(iterator.next()));
    }
    return this;
  }

  public KeyBuilder append(String string)
  {
    if (string != null) {
      output.write(StringUtils.toUtf8(string));
    }
    return this;
  }

  public KeyBuilder append(String first, String... remaining)
  {
    output.write(StringUtils.toUtf8WithNullToEmpty(first));
    for (String string : remaining) {
      output.write(SEPARATOR);
      output.write(StringUtils.toUtf8WithNullToEmpty(string));
    }
    return this;
  }

  public KeyBuilder append(Enum value)
  {
    output.writeByte(value.ordinal());
    return this;
  }

  public KeyBuilder append(EnumSet<?> values)
  {
    for (Enum value : values) {
      output.writeByte(value.ordinal());
    }
    return this;
  }

  public KeyBuilder append(Object value)
  {
    return append(Objects.toString(value, null));
  }

  public KeyBuilder appendIntervals(List<Interval> intervals)
  {
    for (Interval interval : intervals) {
      appendInterval(interval);
    }
    return this;
  }

  public KeyBuilder appendInterval(Interval interval)
  {
    output.writeLong(interval.getStartMillis());
    output.writeLong(interval.getEndMillis());
    return this;
  }

  public KeyBuilder sp()
  {
    output.write(SEPARATOR);
    return this;
  }

  public byte[] build()
  {
    return output.toByteArray();
  }
}
