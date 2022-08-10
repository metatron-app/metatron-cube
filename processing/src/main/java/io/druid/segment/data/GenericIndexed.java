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

import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BufferRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.VLongUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.Tools;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Comparator;
import java.util.Iterator;

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
  public static final byte version = 0x1;   // don't change this

  enum Feature
  {
    SORTED,
    VSIZED_VALUE;

    private final int mask = 1 << ordinal();

    public boolean isSet(int flags) {return (mask & flags) != 0;}

    public int set(int flags, boolean v)
    {
      return v ? flags | mask : flags & ~mask;
    }

    public int getMask() {return mask;}
  }

  public static int features(Feature... features)
  {
    int i = 0;
    for (Feature feature : features) {
      i |= feature.getMask();
    }
    return i;
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
    final int flag = buffer.get();
    ByteBuffer dictionary = ByteBufferSerializer.prepareForRead(buffer);
    return new GenericIndexed<T>(dictionary, strategy, flag);
  }

  public ColumnPartProvider<Dictionary<T>> asColumnPartProvider()
  {
    return asColumnPartProvider(this);
  }

  public static <T> ColumnPartProvider<Dictionary<T>> asColumnPartProvider(Dictionary<T> dictionary)
  {
    return new ColumnPartProvider<Dictionary<T>>()
    {
      @Override
      public int numRows()
      {
        return dictionary.size();
      }

      @Override
      public long getSerializedSize()
      {
        return dictionary.getSerializedSize();
      }

      @Override
      public Dictionary<T> get()
      {
        return dictionary instanceof GenericIndexed ? ((GenericIndexed<T>) dictionary).asSingleThreaded() : dictionary;
      }
    };
  }

  // mostly stick to V1 for v8 format..
  public static <T> GenericIndexed<T> v1(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    return fromIterable(objectsIterable, strategy, 0);
  }

  public static <T> GenericIndexed<T> v2(Iterable<T> objectsIterable, ObjectStrategy<T> strategy)
  {
    return fromIterable(objectsIterable, strategy, Feature.VSIZED_VALUE.mask);
  }

  private static <T> GenericIndexed<T> fromIterable(Iterable<T> objectsIterable, ObjectStrategy<T> strategy, int flag)
  {
    Iterator<T> objects = objectsIterable.iterator();
    if (!objects.hasNext()) {
      return new GenericIndexed<T>(ByteBuffer.wrap(new byte[4]), strategy, flag);
    }

    boolean vsized = Feature.VSIZED_VALUE.isSet(flag);
    boolean allowReverseLookup = strategy instanceof Comparator;
    int count = 0;

    BytesOutputStream offsets = new BytesOutputStream();
    BytesOutputStream values = new BytesOutputStream();

    int offset = 0;
    T prevVal = null;
    do {
      count++;
      T next = objects.next();
      if (allowReverseLookup && prevVal != null && !(((Comparator<T>) strategy).compare(prevVal, next) < 0)) {
        allowReverseLookup = false;
      }

      final byte[] bytes = strategy.toBytes(next);
      if (vsized) {
        values.writeUnsignedVarInt(bytes.length);
        offset += VLongUtils.sizeOfUnsignedVarInt(bytes.length) + bytes.length;
      } else {
        values.writeInt(bytes.length);
        offset += Integer.BYTES + bytes.length;
      }
      offsets.writeInt(offset);
      values.write(bytes);

      if (prevVal instanceof Closeable) {
        CloseQuietly.close((Closeable) prevVal);
      }
      prevVal = next;
    } while (objects.hasNext());

    if (prevVal instanceof Closeable) {
      CloseQuietly.close((Closeable) prevVal);
    }

    ByteBuffer theBuffer = ByteBuffer.allocate(Integer.BYTES + offsets.size() + values.size());
    theBuffer.put(Ints.toByteArray(count));
    theBuffer.put(offsets.asByteBuffer());
    theBuffer.put(values.asByteBuffer());
    theBuffer.flip();

    flag = Feature.SORTED.set(flag, allowReverseLookup);
    return new GenericIndexed<T>(theBuffer.asReadOnlyBuffer(), strategy, flag);
  }

  private final ByteBuffer theBuffer;
  private final ObjectStrategy<T> strategy;
  private final int flag;

  private final int size;
  private final int indexOffset;
  private final int valuesOffset;
  private final BufferIndexed bufferIndexed;

  private GenericIndexed(ByteBuffer buffer, ObjectStrategy<T> strategy, int flag)
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.flag = flag;

    size = theBuffer.getInt();
    indexOffset = theBuffer.position();
    valuesOffset = theBuffer.position() + (size << 2);
    if (Feature.VSIZED_VALUE.isSet(flag)) {
      bufferIndexed = new BufferIndexedV2(() -> theBuffer.asReadOnlyBuffer());
    } else {
      bufferIndexed = new BufferIndexedV1(() -> theBuffer.asReadOnlyBuffer());
    }
  }

  private GenericIndexed(
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      int flag,
      int size,
      int indexOffset,
      int valuesOffset,
      BufferIndexed bufferIndexed
  )
  {
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.flag = flag;
    this.size = size;
    this.indexOffset = indexOffset;
    this.valuesOffset = valuesOffset;
    this.bufferIndexed = bufferIndexed;
  }

  @Override
  public int size()
  {
    return bufferIndexed.size();
  }

  @Override
  public boolean isSorted()
  {
    return Feature.SORTED.isSet(flag);
  }

  @Override
  public Boolean containsNull()
  {
    if (size > 0 && Feature.SORTED.isSet(flag)) {
      return getAsRaw(0).length == 0;
    }
    return null;
  }

  @Override
  public void scan(Tools.Scanner scanner)
  {
    bufferIndexed.scan(scanner);
  }

  @Override
  public T get(int index)
  {
    return bufferIndexed.get(validateIndex(index));
  }

  @Override
  public byte[] getAsRaw(int index)
  {
    return bufferIndexed.getAsRaw(validateIndex(index));
  }

  @Override
  public BufferRef getAsRef(int index)
  {
    return bufferIndexed.getAsRef(validateIndex(index));
  }

  @Override
  public void scan(int index, Tools.Scanner scanner)
  {
    bufferIndexed.scan(validateIndex(index), scanner);
  }

  @Override
  public <R> R apply(int index, Tools.Function<R> function)
  {
    return bufferIndexed.apply(validateIndex(index), function);
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
  public int indexOf(T value, int start)
  {
    return bufferIndexed.indexOf(value, start);
  }

  @Override
  public Iterator<T> iterator()
  {
    return bufferIndexed.iterator();
  }

  @Override
  public long getSerializedSize()
  {
    // see GenericIndexedWriter
    return Byte.BYTES +           // version
           Byte.BYTES +           // flag
           Integer.BYTES +        // numBytesWritten
           Integer.BYTES +        // numElements
           theBuffer.remaining();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{version, (byte) flag}));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + 4)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(size)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  @Override
  public void close()
  {
  }

  private int validateIndex(int index)
  {
    if (index < 0) {
      throw new IAE("Index[%s] < 0", index);
    }
    if (index >= size) {
      throw new IAE(String.format("Index[%s] >= size[%s]", index, size));
    }
    return index;
  }

  public GenericIndexed<T> asSingleThreaded()
  {
    final ByteBuffer buffer = theBuffer.asReadOnlyBuffer();

    return new GenericIndexed<T>(
        buffer,
        strategy,
        flag,
        size,
        indexOffset,
        valuesOffset,
        bufferIndexed.withSupplier(() -> buffer)
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

  /**
   * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
   *
   * @return a non-thread-safe Indexed
   */
  public BufferIndexed singleThreaded()
  {
    return bufferIndexed.withSupplier(() -> theBuffer.asReadOnlyBuffer());
  }

  abstract class BufferIndexed implements Indexed<T>
  {
    final Supplier<ByteBuffer> supplier;

    protected BufferIndexed(Supplier<ByteBuffer> supplier)
    {
      this.supplier = supplier;
    }

    protected abstract BufferIndexed withSupplier(Supplier<ByteBuffer> supplier);

    private int valueOffset(int index)
    {
      return valuesOffset + (index == 0 ? 0 : theBuffer.getInt(indexOffset + (index - 1) * 4));
    }

    protected abstract int valueLength(int offset);

    protected abstract int valueHeaderLength(int length);

    private void scan(Tools.Scanner scanner)
    {
      final ByteBuffer buffer = supplier.get();
      int offset = valuesOffset;
      for (int i = 0; i < size; i++) {
        final int length = valueLength(offset);
        final int header = valueHeaderLength(length);
        scanner.scan(i, buffer, offset + header, length);
        offset += header + length;
      }
    }

    @Override
    public final T get(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(offset);
      if (length == 0) {
        return null;
      }
      final ByteBuffer copyBuffer = supplier.get();
      copyBuffer.position(offset + valueHeaderLength(length));
      return strategy.fromByteBuffer(copyBuffer, length);
    }

    public final byte[] getAsRaw(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(offset);
      if (length == 0) {
        return StringUtils.EMPTY_BYTES;
      }
      final ByteBuffer copyBuffer = supplier.get();
      copyBuffer.position(offset + valueHeaderLength(length));
      byte[] array = new byte[length];
      copyBuffer.get(array);
      return array;
    }

    public final BufferRef getAsRef(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(offset);
      final int header = valueHeaderLength(length);
      return BufferRef.of(theBuffer, offset + header, offset + header + length);
    }

    public final void scan(final int index, final Tools.Scanner function)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(offset);
      final ByteBuffer copyBuffer = supplier.get();
      function.scan(index, copyBuffer, offset + valueHeaderLength(length), length);
    }

    public final <R> R apply(final int index, final Tools.Function<R> function)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(offset);
      final ByteBuffer copyBuffer = supplier.get();
      return function.apply(index, copyBuffer, offset + valueHeaderLength(length), length);
    }

    @Override
    public int size()
    {
      return size;
    }

    @Override
    public int indexOf(T value)
    {
      return indexOf(value, 0);
    }

    @SuppressWarnings("unchecked")
    public int indexOf(T value, int start)
    {
      if (!isSorted() || !(strategy instanceof Comparator)) {
        throw new UnsupportedOperationException("Reverse lookup not allowed.");
      }
      if (StringUtils.isNullOrEmpty(value)) {
        return StringUtils.isNullOrEmpty(get(0)) ? 0 : -1;
      }
      if (strategy instanceof ObjectStrategy.RawComparable) {
        return binarySearchRaw(strategy.toBytes(value), start);
      }
      return binarySearch(value, (Comparator<T>) strategy, start);
    }

    private int binarySearch(final T value, final Comparator<T> comparator, final int start)
    {
      int minIndex = start < 0 ? -(start + 1) : start;
      int maxIndex = size - 1;
      while (minIndex <= maxIndex) {
        final int medianIndex = (minIndex + maxIndex) >>> 1;
        final int comparison = comparator.compare(get(medianIndex), value);
        if (comparison == 0) {
          return medianIndex;
        }
        if (comparison < 0) {
          minIndex = medianIndex + 1;
        } else {
          maxIndex = medianIndex - 1;
        }
      }

      return -(minIndex + 1);
    }

    private int binarySearchRaw(final byte[] target, final int start)
    {
      int minIndex = start < 0 ? -(start + 1) : start;
      int maxIndex = size - 1;
      while (minIndex <= maxIndex) {
        final int medianIndex = (minIndex + maxIndex) >>> 1;
        final int comparison = getAsRef(medianIndex).compareTo(target);
        if (comparison == 0) {
          return medianIndex;
        }
        if (comparison < 0) {
          minIndex = medianIndex + 1;
        } else {
          maxIndex = medianIndex - 1;
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

  private class BufferIndexedV1 extends BufferIndexed
  {
    private BufferIndexedV1(Supplier<ByteBuffer> supplier) {super(supplier);}

    @Override
    protected BufferIndexed withSupplier(Supplier<ByteBuffer> supplier)
    {
      return new BufferIndexedV1(supplier);
    }

    @Override
    protected int valueLength(int offset)
    {
      return theBuffer.getInt(offset);
    }

    @Override
    protected int valueHeaderLength(int length)
    {
      return Integer.BYTES;
    }
  }

  private class BufferIndexedV2 extends BufferIndexed
  {
    private BufferIndexedV2(Supplier<ByteBuffer> supplier) {super(supplier);}

    @Override
    protected BufferIndexed withSupplier(Supplier<ByteBuffer> supplier)
    {
      return new BufferIndexedV2(supplier);
    }

    @Override
    protected int valueLength(int offset)
    {
      return VLongUtils.readUnsignedVarInt(theBuffer, offset);
    }

    @Override
    protected int valueHeaderLength(int length)
    {
      return VLongUtils.sizeOfUnsignedVarInt(length);
    }
  }
}
