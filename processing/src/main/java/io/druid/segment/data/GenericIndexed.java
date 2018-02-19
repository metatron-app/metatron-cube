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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import com.metamx.common.guava.CloseQuietly;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * A generic, flat storage mechanism.  Use static methods fromArray() or fromIterable() to construct.  If input
 * is sorted, supports binary search index lookups.  If input is not sorted, only supports array-like index lookups.
 * <p/>
 * V1 Storage Format:
 * <p/>
 * byte 1: version (0x1)
 * byte 2 == 0x1 =&gt; allowReverseLookup
 * bytes 3-6 =&gt; numBytesUsed
 * bytes 7-10 =&gt; numElements
 * bytes 10-((numElements * 4) + 10): integers representing *end* offsets of byte serialized values
 * bytes ((numElements * 4) + 10)-(numBytesUsed + 2): 4-byte integer representing length of value, followed by bytes for value
 */
public class GenericIndexed<T> implements Indexed<T>, DictionaryLoader<T>, ColumnPartSerde.Serializer
{
  private static final byte version = 0x1;

  public static <T> GenericIndexed<T> fromArray(T[] objects, ObjectStrategy<T> strategy)
  {
    return fromIterable(Arrays.asList(objects), strategy);
  }

  public static <T> GenericIndexed<T> fromIterable(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      final ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0);
      buffer.flip();
      return new GenericIndexed<T>(buffer, strategy, true);
    }

    boolean allowReverseLookup = !(strategy instanceof ObjectStrategy.NotComparable);
    int count = 0;

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    try {
      int offset = 0;
      T prevVal = null;
      do {
        count++;
        T next = objects.next();
        if (allowReverseLookup && prevVal != null && !(strategy.compare(prevVal, next) < 0)) {
          allowReverseLookup = false;
        }

        final byte[] bytes = strategy.toBytes(next);
        offset += 4 + bytes.length;
        headerBytes.write(Ints.toByteArray(offset));
        valueBytes.write(Ints.toByteArray(bytes.length));
        valueBytes.write(bytes);

        if (prevVal instanceof Closeable) {
          CloseQuietly.close((Closeable) prevVal);
        }
        prevVal = next;
      } while (objects.hasNext());

      if (prevVal instanceof Closeable) {
        CloseQuietly.close((Closeable) prevVal);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(Ints.BYTES + headerBytes.size() + valueBytes.size());
    theBuffer.put(Ints.toByteArray(count));
    theBuffer.put(headerBytes.toByteArray());
    theBuffer.put(valueBytes.toByteArray());
    theBuffer.flip();

    return new GenericIndexed<T>(theBuffer.asReadOnlyBuffer(), strategy, allowReverseLookup);
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return bufferIndexed.getClazz();
  }

  @Override
  public int size()
  {
    return bufferIndexed.size();
  }

  @Override
  public void collect(Collector<T> collector)
  {
    final ByteBuffer buffer = bufferAsReadOnly();
    for (int i = 0; i < size; i++) {
      collector.collect(i, loadValue(buffer, i));
    }
  }

  @Override
  public T get(int index)
  {
    return bufferIndexed.get(index);
  }

  public byte[] getAsRaw(int index)
  {
    return bufferIndexed.getAsRaw(index);
  }

  /**
   * Returns the index of "value" in this GenericIndexed object, or (-(insertion point) - 1) if the value is not
   * present, in the manner of Arrays.binarySearch. This strengthens the contract of Indexed, which only guarantees
   * that values-not-found will return some negative number.
   *
   * @param value value to search for
   *
   * @return index of value, or negative number equal to (-(insertion point) - 1).
   */
  @Override
  public int indexOf(T value)
  {
    return bufferIndexed.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return bufferIndexed.iterator();
  }

  public int totalLengthOfWords()
  {
    return theBuffer.getInt(indexOffset + (size - 1) * 4) - size * 4;
  }

  final ByteBuffer theBuffer;
  final ObjectStrategy<T> strategy;
  final boolean allowReverseLookup;

  final int size;
  final int indexOffset;
  final int valuesOffset;
  final BufferIndexed bufferIndexed;

  GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;

    size = theBuffer.getInt();
    indexOffset = theBuffer.position();
    valuesOffset = theBuffer.position() + (size << 2);
    bufferIndexed = new BufferIndexed();
  }

  GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup,
      int size,
      int indexOffset,
      int valuesOffset,
      BufferIndexed bufferIndexed
  )
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.size = size;
    this.indexOffset = indexOffset;
    this.valuesOffset = valuesOffset;
    this.bufferIndexed = bufferIndexed;
  }

  public GenericIndexed<T> asSingleThreaded()
  {
    final ByteBuffer copyBuffer = theBuffer.asReadOnlyBuffer();
    BufferIndexed bufferIndexed = new BufferIndexed()
    {
      @Override
      protected ByteBuffer reader()
      {
        return copyBuffer;
      }
    };
    return new GenericIndexed<T>(
        copyBuffer,
        strategy,
        allowReverseLookup,
        size,
        indexOffset,
        valuesOffset,
        bufferIndexed
    )
    {
      private int cacheId = -1;
      private T cached;

      @Override
      public T get(int index)
      {
        if (index != cacheId) {
          cached = bufferIndexed.get(cacheId = index);
        }
        return cached;
      }
    };
  }

  protected final ByteBuffer bufferAsReadOnly()
  {
    return theBuffer.asReadOnlyBuffer();
  }

  protected final T loadValue(ByteBuffer buffer, int i)
  {
    return bufferIndexed.loadValue(buffer, i);
  }

  class BufferIndexed implements Indexed<T>
  {
    @Override
    public Class<? extends T> getClazz()
    {
      return strategy.getClazz();
    }

    @Override
    public int size()
    {
      return size;
    }

    protected ByteBuffer reader()
    {
      return bufferAsReadOnly();
    }

    public final T get(final int index)
    {
      final ByteBuffer copyBuffer = reader();
      if (index < 0) {
        throw new IAE("Index[%s] < 0", index);
      }
      if (index >= size) {
        throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
      }

      return loadValue(copyBuffer, index);
    }

    public final byte[] getAsRaw(final int index)
    {
      final ByteBuffer copyBuffer = reader();
      final int startOffset;
      final int endOffset;

      if (index == 0) {
        startOffset = 4;
        endOffset = copyBuffer.getInt(indexOffset);
      } else {
        copyBuffer.position(indexOffset + ((index - 1) * 4));
        startOffset = copyBuffer.getInt() + 4;
        endOffset = copyBuffer.getInt();
      }

      if (startOffset == endOffset) {
        return StringUtils.EMPTY_BYTES;
      }
      copyBuffer.position(valuesOffset + startOffset);
      byte[] array = new byte[endOffset - startOffset];
      copyBuffer.get(array);
      return array;
    }

    private T loadValue(final ByteBuffer copyBuffer, final int index)
    {
      final int startOffset;
      final int endOffset;

      if (index == 0) {
        startOffset = 4;
        endOffset = copyBuffer.getInt(indexOffset);
      } else {
        copyBuffer.position(indexOffset + ((index - 1) * 4));
        startOffset = copyBuffer.getInt() + 4;
        endOffset = copyBuffer.getInt();
      }

      if (startOffset == endOffset) {
        return null;
      }

      copyBuffer.position(valuesOffset + startOffset);

      // fromByteBuffer must not modify the buffer limit
      return strategy.fromByteBuffer(copyBuffer, endOffset - startOffset);
    }

    @Override
    public int indexOf(T value)
    {
      if (!allowReverseLookup) {
        throw new UnsupportedOperationException("Reverse lookup not allowed.");
      }

      value = (value != null && value.equals("")) ? null : value;

      int minIndex = 0;
      int maxIndex = size - 1;
      while (minIndex <= maxIndex) {
        int currIndex = (minIndex + maxIndex) >>> 1;

        T currValue = GenericIndexed.this.get(currIndex);
        int comparison = strategy.compare(currValue, value);
        if (comparison == 0) {
          return currIndex;
        }

        if (comparison < 0) {
          minIndex = currIndex + 1;
        } else {
          maxIndex = currIndex - 1;
        }
      }

      return -(minIndex + 1);
    }

    @Override
    public Iterator<T> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
  }

  public long getSerializedSize()
  {
    return theBuffer.remaining() + 2 + 4 + 4;
  }

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, allowReverseLookup ? (byte) 0x1 : (byte) 0x0}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }

  /**
   * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
   *
   * @return a non-thread-safe Indexed
   */
  public GenericIndexed<T>.BufferIndexed singleThreaded()
  {
    final ByteBuffer copyBuffer = theBuffer.asReadOnlyBuffer();
    return new BufferIndexed()
    {
      @Override
      protected ByteBuffer reader()
      {
        return copyBuffer;
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static <T> GenericIndexed<T> read(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy
  )
  {
    byte versionFromBuffer = buffer.get();

    if (version == versionFromBuffer) {
      boolean allowReverseLookup = buffer.get() == 0x1;
      int size = buffer.getInt();
      ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
      bufferToUse.limit(bufferToUse.position() + size);
      buffer.position(bufferToUse.limit());
      return new GenericIndexed<T>(
          bufferToUse,
          strategy,
          allowReverseLookup
      );
    }

    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  public static final ObjectStrategy<String> STRING_STRATEGY = new CacheableObjectStrategy<String>()
  {
    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
    {
      return StringUtils.fromUtf8(buffer, numBytes);
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return StringUtils.toUtf8(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return GuavaUtils.nullFirstNatural().compare(o1, o2);
    }
  };

  public static final ObjectStrategy<String> STRING_WITH_INTERN_STRATEGY = new CacheableObjectStrategy<String>()
  {
    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
    {
      return StringUtils.fromUtf8(buffer, numBytes).intern();
    }

    @Override
    public byte[] toBytes(String val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return StringUtils.toUtf8(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return GuavaUtils.nullFirstNatural().compare(o1, o2);
    }
  };
}
