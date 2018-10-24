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
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.sketch.SketchOp;
import io.druid.query.sketch.TypedSketch;
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
  static final byte version = 0x1;

  enum Feature
  {
    SORTED, QUANTILE_SKETCH, THETA_SKETCH;

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
      return new GenericIndexed<T>(null, null, buffer, strategy, true);
    }

    boolean sorted = !(strategy instanceof ObjectStrategy.NotComparable);
    int count = 0;

    ByteArrayOutputStream headerBytes = new ByteArrayOutputStream();
    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    try {
      int offset = 0;
      T prevVal = null;
      do {
        count++;
        T next = objects.next();
        if (sorted && prevVal != null && !(strategy.compare(prevVal, next) < 0)) {
          sorted = false;
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

    return new GenericIndexed<T>(null, null, theBuffer.asReadOnlyBuffer(), strategy, sorted);
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
    for (int i = 0; i < count; i++) {
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
    return theBuffer.getInt(indexOffset + (count - 1) * 4) - count * 4;
  }

  final ByteBuffer quantile;
  final ByteBuffer theta;
  final ByteBuffer theBuffer;
  final ObjectStrategy<T> strategy;
  final boolean allowReverseLookup;

  final int count;
  final int indexOffset;
  final int valuesOffset;
  final BufferIndexed bufferIndexed;

  GenericIndexed(
      ByteBuffer quantile,
      ByteBuffer theta,
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup
  )
  {
    this.quantile = quantile;
    this.theta = theta;
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;

    count = theBuffer.getInt();
    indexOffset = theBuffer.position();
    valuesOffset = theBuffer.position() + (count << 2);
    bufferIndexed = new BufferIndexed();
  }

  GenericIndexed(
      ByteBuffer quantile,
      ByteBuffer theta,
      ByteBuffer buffer,
      ObjectStrategy<T> strategy,
      boolean allowReverseLookup,
      int count,
      int indexOffset,
      int valuesOffset,
      BufferIndexed bufferIndexed
  )
  {
    this.quantile = quantile;
    this.theta = theta;
    this.theBuffer = buffer;
    this.strategy = strategy;
    this.allowReverseLookup = allowReverseLookup;
    this.count = count;
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
        quantile,
        theta,
        copyBuffer,
        strategy,
        allowReverseLookup,
        count,
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
      return count;
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
      if (index >= count) {
        throw new IAE(String.format("Index[%s] >= size[%s]", index, count));
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
      int maxIndex = count - 1;
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
    if (quantile != null) {
      length += Ints.BYTES + quantile.remaining();  // length + binary
    }
    if (theta != null) {
      length += Ints.BYTES + theta.remaining();  // length + binary
    }
    length += Ints.BYTES + theBuffer.remaining();   // length + binary
    length += Ints.BYTES; // count
    return length;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    byte flag = 0;
    if (allowReverseLookup) {
      flag |= Feature.SORTED.getMask();
    }
    if (quantile != null) {
      flag |= Feature.QUANTILE_SKETCH.getMask();
    }
    if (theta != null) {
      flag |= Feature.THETA_SKETCH.getMask();
    }
    channel.write(ByteBuffer.wrap(new byte[]{version, flag}));
    if (quantile != null) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(quantile.remaining())));
      channel.write(quantile.asReadOnlyBuffer());
    }
    if (theta != null) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(theta.remaining())));
      channel.write(theta.asReadOnlyBuffer());
    }
    channel.write(ByteBuffer.wrap(Ints.toByteArray(theBuffer.remaining() + Ints.BYTES)));
    channel.write(ByteBuffer.wrap(Ints.toByteArray(count)));
    channel.write(theBuffer.asReadOnlyBuffer());
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }

  public boolean isSorted()
  {
    return allowReverseLookup;
  }

  public boolean hasQuantile()
  {
    return quantile != null;
  }

  @SuppressWarnings("unchecked")
  public ItemsSketch<String> getQuantile()
  {
    // todo handle ValueDesc
    return quantile == null ? null : (ItemsSketch) TypedSketch.readPart(quantile, SketchOp.QUANTILE, ValueDesc.STRING);
  }

  public boolean hasTheta()
  {
    return theta != null;
  }

  public Sketch getTheta()
  {
    // todo handle ValueDesc
    return theta == null ? null : (Sketch) TypedSketch.readPart(theta, SketchOp.THETA, ValueDesc.STRING);
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

  public static <T> GenericIndexed<T> read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    final byte versionFromBuffer = buffer.get();
    if (version != versionFromBuffer) {
      throw new IAE("Unknown version[%s]", versionFromBuffer);
    }

    byte flag = buffer.get();
    boolean sorted = Feature.SORTED.isSet(flag);
    boolean hasQuantileSketch = Feature.QUANTILE_SKETCH.isSet(flag);
    boolean hasThetaSketch = Feature.THETA_SKETCH.isSet(flag);
    ByteBuffer quantile = null;
    if (hasQuantileSketch) {
      quantile = ByteBufferSerializer.prepareForRead(buffer);
    }
    ByteBuffer theta = null;
    if (hasThetaSketch) {
      theta = ByteBufferSerializer.prepareForRead(buffer);
    }
    ByteBuffer dictionary = ByteBufferSerializer.prepareForRead(buffer);
    return new GenericIndexed<T>(quantile, theta, dictionary, strategy, sorted);
  }
}
