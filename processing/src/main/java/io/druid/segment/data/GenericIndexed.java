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

import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.ByteArrayOutputStream;
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
public class GenericIndexed<T> implements Dictionary<T>, ColumnPartSerde.Serializer
{
  public static final byte version = 0x1;

  enum Feature
  {
    SORTED;

    public boolean isSet(int flags) { return (getMask() & flags) != 0; }

    public int getMask() { return (1 << ordinal()); }
  }

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
  public int size()
  {
    return bufferIndexed.size();
  }

  @Override
  public T get(int index)
  {
    return bufferIndexed.get(index);
  }

  @Override
  public Boolean containsNull()
  {
    if (allowReverseLookup) {
      return getAsRaw(0).length == 0;
    }
    return null;
  }

  @Override
  public byte[] getAsRaw(int index)
  {
    return bufferIndexed.getAsRaw(index);
  }

  @Override
  public int copyTo(int index, BytesOutputStream output)
  {
    return bufferIndexed.copyTo(index, output);
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

  @Override
  public int sizeOfWords()
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
    bufferIndexed = new BufferIndexed() {
      @Override
      protected ByteBuffer bufferForRead()
      {
        return theBuffer.asReadOnlyBuffer();
      }
    };
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
    final BufferIndexed bufferIndexed = new BufferIndexed()
    {
      @Override
      protected ByteBuffer bufferForRead()
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

  abstract class BufferIndexed implements Indexed<T>
  {
    @Override
    public int size()
    {
      return size;
    }

    protected abstract ByteBuffer bufferForRead();

    @Override
    public final T get(final int index)
    {
      final ByteBuffer copyBuffer = bufferForRead();
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
      final ByteBuffer copyBuffer = bufferForRead();
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

    public final int copyTo(final int index, final BytesOutputStream output)
    {
      final ByteBuffer copyBuffer = bufferForRead();
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

      final int length = endOffset - startOffset;
      if (length > 0) {
        copyBuffer.position(valuesOffset + startOffset);
        copyBuffer.get(output.unwrap(), 0, length);
      }
      return length;
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

  @Override
  public long getSerializedSize()
  {
    long length = 0;
    length += Byte.BYTES; // version
    length += Byte.BYTES; // flag
    length += Ints.BYTES + theBuffer.remaining();   // length + binary
    length += Ints.BYTES; // count
    return length;
  }

  @Override
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

  @Override
  public void close() throws IOException
  {
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
      protected ByteBuffer bufferForRead()
      {
        return copyBuffer;
      }
    };
  }

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    final byte versionFromBuffer = buffer.get();
    if (version != versionFromBuffer) {
      throw new IAE("Unknown version[%s]", versionFromBuffer);
    }
    return readIndex(buffer, strategy);
  }

  public static <T> GenericIndexed<T> readIndex(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    byte flag = buffer.get();
    boolean sorted = Feature.SORTED.isSet(flag);
    ByteBuffer dictionary = ByteBufferSerializer.prepareForRead(buffer);
    return new GenericIndexed<T>(dictionary, strategy, sorted);
  }

  public ColumnPartProvider<Dictionary<T>> asColumnPartProvider()
  {
    return new ColumnPartProvider<Dictionary<T>>()
    {
      @Override
      public int numRows()
      {
        return size();
      }

      @Override
      public long getSerializedSize()
      {
        return GenericIndexed.this.getSerializedSize();
      }

      @Override
      public Dictionary<T> get()
      {
        return asSingleThreaded();
      }
    };
  }
}
