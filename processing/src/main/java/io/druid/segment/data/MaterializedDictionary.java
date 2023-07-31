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

import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BufferRef;
import io.druid.common.utils.StringUtils;
import io.druid.segment.Tools;
import io.druid.segment.bitmap.IntIterators;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MaterializedDictionary implements Dictionary<String>
{
  public static MaterializedDictionary sorted(String[] values)
  {
    return new MaterializedDictionary(Feature.SORTED.mask, values);
  }

  private final int flag;
  private final String[] values;
  private final byte[][] bytes;

  public MaterializedDictionary(int flag, String[] values)
  {
    this.flag = flag;
    this.values = values;
    this.bytes = Arrays.stream(values).map(StringUtils::toUtf8).toArray(x -> new byte[x][]);
  }

  @Override
  public void scan(IntIterator iterator, Tools.ObjectScanner<String> scanner)
  {
    if (iterator == null) {
      for (int i = 0; i < values.length; i++) {
        scanner.scan(i, values[i]);
      }
    } else {
      while (iterator.hasNext()) {
        int i = iterator.next();
        scanner.scan(i, values[i]);
      }
    }
  }

  @Override
  public void scan(IntIterator iterator, Tools.Scanner scanner)
  {
    if (iterator == null) {
      iterator = IntIterators.fromTo(0, values.length);
    }
    IntIterators.forEach(iterator, asConsumer(scanner));
  }

  private IntConsumer asConsumer(Tools.Scanner scanner)
  {
    return x -> scanner.scan(x, ByteBuffer.wrap(bytes[x]), 0, bytes[x].length);
  }

  private <T> IntFunction<T> asFunction(Tools.Function<T> scanner)
  {
    return x -> scanner.apply(x, ByteBuffer.wrap(bytes[x]), 0, bytes[x].length);
  }

  @Override
  public <R> Stream<R> apply(IntIterator iterator, Tools.Function<R> function)
  {
    Iterator<R> it = IntIterators.apply(iterator, asFunction(function));
    if (iterator == null) {
      return StreamSupport.stream(Spliterators.spliterator(it, values.length, 0), false);
    } else {
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false);
    }
  }

  @Override
  public int flag()
  {
    return flag;
  }

  @Override
  public Boolean containsNull()
  {
    return Arrays.stream(values).anyMatch(StringUtils::isNullOrEmpty);
  }

  @Override
  public long getSerializedSize()
  {
    return 0;
  }

  @Override
  public void close()
  {
  }

  @Override
  public int size()
  {
    return values.length;
  }

  @Override
  public String get(int index)
  {
    return values[index];
  }

  @Override
  public BufferRef getAsRef(int index)
  {
    return BufferRef.of(ByteBuffer.wrap(bytes[index]), 0, bytes[index].length);
  }

  @Override
  public void scan(int index, Tools.Scanner scanner)
  {
    scanner.scan(index, ByteBuffer.wrap(bytes[index]), 0, bytes[index].length);
  }

  @Override
  public <R> R apply(int index, Tools.Function<R> function)
  {
    return function.apply(index, ByteBuffer.wrap(bytes[index]), 0, bytes[index].length);
  }

  @Override
  public int indexOf(String value, int start, int end, boolean binary)
  {
    if (binary) {
      return Arrays.binarySearch(values, start, end, value);
    }
    for (int i = start; i < end; i++) {
      if (values[i].equals(value)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int indexOf(BinaryRef bytes, int start, int end, boolean binary)
  {
    return indexOf(bytes.toUTF8(), start, end, binary);
  }

  @Override
  public Iterator<String> iterator()
  {
    return Arrays.asList(values).iterator();
  }
}
