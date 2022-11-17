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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import io.druid.collections.IntList;
import io.druid.common.guava.BinaryRef;
import io.druid.common.guava.BufferRef;
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.BytesWindow;
import io.druid.common.utils.StringUtils;
import io.druid.data.VLongUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.Tools;
import io.druid.segment.serde.ColumnPartSerde;
import org.roaringbitmap.IntIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
  public static <T> Indexed<T> asSingleThreaded(Indexed<T> indexed)
  {
    return indexed instanceof GenericIndexed ? ((GenericIndexed<T>) indexed).asSingleThreaded() : indexed;
  }

  public static final byte version = 0x1;   // don't change this

  enum Feature
  {
    SORTED,
    VSIZED_VALUE,
    NO_OFFSET;

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

  public static GenericIndexed<String> readString(ByteBuffer buffer)
  {
    return read(buffer, ObjectStrategy.STRING_STRATEGY);
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

  @SuppressWarnings("unchecked")
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
  private final ObjectStrategy<T> theStrategy;
  private final int flag;

  private final int size;
  private final int indexOffset;
  private final int valuesOffset;
  private final BufferIndexed bufferIndexed;

  private GenericIndexed(ByteBuffer buffer, ObjectStrategy<T> strategy, int flag)
  {
    this.theBuffer = buffer;
    this.theStrategy = strategy;
    this.flag = flag;

    Supplier<ByteBuffer> supplier = () -> theBuffer.asReadOnlyBuffer();

    size = theBuffer.getInt();
    if (Feature.NO_OFFSET.isSet(flag)) {
      Preconditions.checkArgument(Feature.SORTED.isSet(flag), "Not sorted?");
      final boolean hasNull = theBuffer.get() != 0;
      final int minLength = VLongUtils.readUnsignedVarInt(theBuffer);
      final int maxLength = VLongUtils.readUnsignedVarInt(theBuffer);
      indexOffset = valuesOffset = theBuffer.position();
      if (minLength == maxLength) {
        bufferIndexed = new Fixed(hasNull, maxLength, strategy, supplier);
      } else {
        bufferIndexed = new NoOffset(hasNull, minLength, maxLength, strategy, supplier);
      }
    } else {
      indexOffset = theBuffer.position();
      valuesOffset = theBuffer.position() + (size << 2);
      if (Feature.VSIZED_VALUE.isSet(flag)) {
        bufferIndexed = new BufferIndexedV2(strategy, supplier);
      } else {
        bufferIndexed = new BufferIndexedV1(strategy, supplier);
      }
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
    this.theStrategy = strategy;
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
  public int flag()
  {
    return flag;
  }

  @Override
  public Boolean containsNull()
  {
    return bufferIndexed.hasNull();
  }

  @Override
  public void scan(Tools.Scanner scanner)
  {
    bufferIndexed.scan(scanner);
  }

  @Override
  public void scan(IntIterator iterator, Tools.Scanner scanner)
  {
    if (iterator == null) {
      bufferIndexed.scan(scanner);
    } else {
      bufferIndexed.scan(iterator, scanner);
    }
  }

  @Override
  public void scan(int index, Tools.Scanner scanner)
  {
    bufferIndexed.scan(validateIndex(index), scanner);
  }

  @Override
  public void scan(Tools.ObjectScanner<T> scanner)
  {
    bufferIndexed.scan(scanner);
  }

  @Override
  public void scan(IntIterator iterator, Tools.ObjectScanner<T> scanner)
  {
    if (iterator == null) {
      bufferIndexed.scan(scanner);
    } else {
      bufferIndexed.scan(iterator, scanner);
    }
  }

  @Override
  public <R> Stream<R> apply(Tools.Function<R> function)
  {
    return bufferIndexed.stream(function);
  }

  @Override
  public T get(int index)
  {
    return bufferIndexed.get(validateIndex(index));
  }

  public T get(int index, T previous)
  {
    return bufferIndexed.get(validateIndex(index), previous);
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
  public int indexOf(T value, int start, int end, boolean binary)
  {
    return bufferIndexed.indexOf(value, start, end, binary);
  }

  private static final int TRIVIAL = 8;
  private static final double LOG2 = Math.log(2);

  @Override
  @SuppressWarnings("unchecked")
  public IntStream indexOf(List<T> values)
  {
    if (values.size() < TRIVIAL || size < TRIVIAL) {
      return Dictionary.super.indexOf(values);
    }
    if (!isSorted() || !(theStrategy instanceof Comparator)) {
      throw new UnsupportedOperationException("Reverse lookup not allowed.");
    }
    final Comparator<T> comparator = (Comparator<T>) theStrategy;

    int ds = 0;
    int de = size;
    int vs = 0;
    int ve = values.size();

    final IntList prefix = new IntList(2);

    T d = get(ds);
    T v = values.get(vs);
    if (StringUtils.isNullOrEmpty(v)) {
      if (d == null) {
        prefix.add(ds);
        d = get(++ds);
      }
      v = values.get(++vs);
    } else if (d == null) {
      d = get(++ds);
    }
    int compare = comparator.compare(v, d);
    if (compare == 0) {
      prefix.add(ds);
      ds++;
      vs++;
    } else if (compare < 0) {
      vs = Collections.binarySearch(values, d, comparator);
      if (vs < 0) {
        vs = -vs - 1;
      }
    } else {
      ds = indexOf(v, ds + 1, de, true);
      if (ds < 0) {
        ds = -ds - 1;
      } else {
        prefix.add(ds);
        ds++;
        vs++;
      }
    }
    if (vs >= ve || ds >= de) {
      return prefix.stream();
    }

    final IntList postfix = new IntList(1);

    int di = de - 1;
    int vi = ve - 1;

    d = get(di);
    v = values.get(vi);
    compare = comparator.compare(v, d);
    if (compare == 0) {
      postfix.add(di);
      de--;
      ve--;
    } else if (compare > 0) {
      vi = Collections.binarySearch(values, d, comparator);
      ve = vi < 0 ? -vi - 1 : vi + 1;
    } else {
      di = indexOf(v, ds, de - 1, true);
      if (di < 0) {
        de = -di - 1;
      } else {
        postfix.add(di);
        de = di;
        ve--;
      }
    }
    if (vs >= ve || ds >= de) {
      return IntStream.concat(prefix.stream(), postfix.stream());
    }

    IntStream stream = IntStream.range(vs, ve).map(searchOp(values, vs, ve, ds, de)).filter(x -> x >= 0);
    if (!prefix.isEmpty()) {
      stream = IntStream.concat(prefix.stream(), stream);
    }
    if (!postfix.isEmpty()) {
      stream = IntStream.concat(stream, postfix.stream());
    }
    return stream;
  }

  private IntUnaryOperator searchOp(List<T> values, int vs, int ve, int ds, int de)
  {
    if (theStrategy instanceof ObjectStrategy.RawComparable) {
      return new Searcher<byte[]>(vs, ve, ds, de)
      {
        private final BytesWindow window = new BytesWindow();
        private final List<byte[]> bytes = values.subList(vs, ve).stream().map(v -> theStrategy.toBytes(v))
                                                 .collect(Collectors.toList());

        @Override
        protected int search(int vi, int start, int end, boolean binary)
        {
          return bufferIndexed._rawIndexOf(window.set(bytes.get(vi - vs)), start, end, binary);
        }
      };
    }
    return new Searcher<T>(vs, ve, ds, de)
    {
      @Override
      protected int search(int vi, int start, int end, boolean binary)
      {
        return bufferIndexed._indexOf(values.get(vi), start, end, binary);
      }
    };
  }

  private static abstract class Searcher<T> implements IntUnaryOperator
  {
    private final int vs;
    private final int ve;
    private final int de;

    private int ds;
    private boolean binary;

    private Searcher(int vs, int ve, int ds, int de)
    {
      this.vs = vs;
      this.ve = ve;
      this.ds = ds;
      this.de = de;
    }

    @Override
    public int applyAsInt(final int vi)
    {
      final int di = ds < 0 ? -ds - 1 : ds;
      if (vi % 100 == 0 && di < de) {
        binary = (de - di) * LOG2 > (ve - vi) * Math.log(de - di + 1);
      }
      return ds = search(vi, di, de, vi == vs || binary);
    }

    protected abstract int search(int vi, int start, int end, boolean binary);
  }

  @Override
  public int indexOf(BinaryRef bytes, int start, int end, boolean binary)
  {
    return bufferIndexed.indexOf(bytes, start, end, binary);
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

  public boolean isRecyclable()
  {
    return false;
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
    return new GenericIndexed<T>(
        theBuffer,
        theStrategy,
        flag,
        size,
        indexOffset,
        valuesOffset,
        bufferIndexed.asSingleThreaded()
    )
    {
      private int vcacheId = -1;
      private T vcached;

      private int bcacheId = -1;
      private byte[] bcached;

      @Override
      public T get(int index)
      {
        if (index != vcacheId) {
          vcached = super.get(vcacheId = index);
        }
        return vcached;
      }

      @Override
      public byte[] getAsRaw(final int index)
      {
        if (index != bcacheId) {
          bcached = super.getAsRaw(bcacheId = index);
        }
        return bcached;
      }

      @Override
      public boolean isRecyclable()
      {
        return bufferIndexed.strategy instanceof ObjectStrategy.Recycling;
      }
    };
  }

  private ObjectStrategy<T> dedicatedStrategy()
  {
    return ObjectStrategies.singleThreaded(theStrategy);
  }

  abstract class BufferIndexed implements Indexed<T>
  {
    final ObjectStrategy<T> strategy;
    final Supplier<ByteBuffer> supplier;

    protected BufferIndexed(ObjectStrategy<T> strategy, Supplier<ByteBuffer> supplier)
    {
      this.strategy = strategy;
      this.supplier = supplier;
    }

    protected Boolean hasNull()
    {
      if (size > 0 && Feature.SORTED.isSet(flag)) {
        return valueLength(0, valuesOffset) == 0;
      }
      return null;
    }

    protected abstract BufferIndexed asSingleThreaded();

    protected int valueOffset(int index)
    {
      return valuesOffset + (index == 0 ? 0 : theBuffer.getInt(indexOffset + (index - 1) * 4));
    }

    protected abstract int valueLength(int index, int offset);

    protected abstract int valueHeaderLength(int index, int length);

    private void scan(Tools.Scanner scanner)
    {
      final ByteBuffer buffer = supplier.get();
      int offset = valuesOffset;
      for (int index = 0; index < size; index++) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        scanner.scan(index, buffer, offset + header, length);
        offset += scanDelta(index, length, header);
      }
    }

    private void scan(IntIterator iterator, Tools.Scanner scanner)
    {
      if (!iterator.hasNext()) {
        return;
      }
      final ByteBuffer buffer = supplier.get();
      int index = iterator.next();
      int offset = valueOffset(index);
      while (true) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        scanner.scan(index, buffer, offset + header, length);
        if (!iterator.hasNext()) {
          return;
        }
        final int next = iterator.next();
        if (next == index + 1) {
          offset += scanDelta(index, length, header);
        } else {
          offset = valueOffset(next);
        }
        index = next;
      }
    }

    private void scan(Tools.ObjectScanner<T> scanner)
    {
      final ByteBuffer buffer = supplier.get();
      int offset = valuesOffset;
      for (int index = 0; index < size; index++) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        buffer.limit(offset + header + length).position(offset + header);
        scanner.scan(index, strategy.fromByteBuffer(buffer));
        offset += scanDelta(index, length, header);
      }
    }

    private void scan(IntIterator iterator, Tools.ObjectScanner<T> scanner)
    {
      if (!iterator.hasNext()) {
        return;
      }
      final ByteBuffer buffer = supplier.get();
      int index = iterator.next();
      int offset = valueOffset(index);
      while (true) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        buffer.limit(offset + header + length).position(offset + header);
        scanner.scan(index, strategy.fromByteBuffer(buffer));
        if (!iterator.hasNext()) {
          return;
        }
        final int next = iterator.next();
        if (next == index + 1) {
          offset += scanDelta(index, length, header);
        } else {
          offset = valueOffset(next);
        }
        index = next;
      }
    }

    private <R> Stream<R> stream(Tools.Function<R> function)
    {
      Iterator<R> iterator = new Iterator<R>()
      {
        private final ByteBuffer buffer = supplier.get();
        private int index;
        private int offset = valuesOffset;

        @Override
        public boolean hasNext()
        {
          return index < size;
        }

        @Override
        public R next()
        {
          final int length = valueLength(index, offset);
          final int header = valueHeaderLength(index, length);
          final R ret = function.apply(index, buffer, offset + header, length);
          offset += scanDelta(index, length, header);
          index++;
          return ret;
        }
      };
      return StreamSupport.stream(Spliterators.spliterator(iterator, size, 0), false);
    }

    protected int scanDelta(int index, int length, int header)
    {
      return header + length;
    }

    @Override
    public final T get(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      if (length == 0) {
        return null;
      }
      return getValue(index, offset, length);
    }

    private T getValue(int index, int offset, int length)
    {
      final ByteBuffer copyBuffer = supplier.get();
      copyBuffer.position(offset + valueHeaderLength(index, length));
      return strategy.fromByteBuffer(copyBuffer, length);
    }

    public final T get(final int index, final T prev)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      if (length == 0) {
        return null;
      }
      return getValue(index, offset, length, prev);
    }

    private T getValue(int index, int offset, int length, final T prev)
    {
      final ByteBuffer copyBuffer = supplier.get();
      copyBuffer.position(offset + valueHeaderLength(index, length));
      return (((ObjectStrategy.Recycling<T>) strategy).fromByteBuffer(copyBuffer, length, prev));
    }

    public final byte[] getAsRaw(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      if (length == 0) {
        return StringUtils.EMPTY_BYTES;
      }
      final ByteBuffer copyBuffer = supplier.get();
      copyBuffer.position(offset + valueHeaderLength(index, length));
      byte[] array = new byte[length];
      copyBuffer.get(array);
      return array;
    }

    public final BufferRef getAsRef(final int index)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      final int header = valueHeaderLength(index, length);
      return BufferRef.of(theBuffer, offset + header, length);
    }

    private int[] toIndices(final int index, final int[] reuse)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      final int header = valueHeaderLength(index, length);
      reuse[0] = offset + header;
      reuse[1] = length;
      return reuse;
    }

    public final void scan(final int index, final Tools.Scanner function)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      final ByteBuffer copyBuffer = supplier.get();
      function.scan(index, copyBuffer, offset + valueHeaderLength(index, length), length);
    }

    public final <R> R apply(final int index, final Tools.Function<R> function)
    {
      final int offset = valueOffset(index);
      final int length = valueLength(index, offset);
      final ByteBuffer copyBuffer = supplier.get();
      return function.apply(index, copyBuffer, offset + valueHeaderLength(index, length), length);
    }

    @Override
    public int size()
    {
      return size;
    }

    @Override
    public int indexOf(T value)
    {
      return indexOf(value, 0, size, true);
    }

    public int indexOf(T value, int start, int end, boolean binary)
    {
      if (!isSorted() || !(strategy instanceof Comparator)) {
        throw new UnsupportedOperationException("Reverse lookup not allowed.");
      }
      if (StringUtils.isNullOrEmpty(value)) {
        return start == 0 && StringUtils.isNullOrEmpty(get(0)) ? 0 : -1;
      }
      if (strategy instanceof ObjectStrategy.RawComparable) {
        return _rawIndexOf(new BytesRef(strategy.toBytes(value)), start, end, binary);
      }
      return _indexOf(value, start, end, binary);
    }

    private int _rawIndexOf(BinaryRef value, int start, int end, boolean binary)
    {
      return binary ? binarySearchRaw(value, start, end) : linearSearchRaw(value, start, end);
    }

    @SuppressWarnings("unchecked")
    private int _indexOf(T value, int start, int end, boolean binary)
    {
      final Comparator<T> comparator = (Comparator<T>) strategy;
      return binary ? binarySearch(value, comparator, start, end) : linearSearch(value, comparator, start, end);
    }

    public int indexOf(BinaryRef bytes, int start, int end, boolean binary)
    {
      if (!isSorted() || !(strategy instanceof Comparator)) {
        throw new UnsupportedOperationException("Reverse lookup not allowed.");
      }
      if (bytes.length() == 0) {
        return start == 0 && StringUtils.isNullOrEmpty(get(0)) ? 0 : -1;
      }
      if (strategy instanceof ObjectStrategy.RawComparable) {
        return binary ? binarySearchRaw(bytes, start, end) : linearSearchRaw(bytes, start, end);
      }
      final T find = strategy.fromByteBuffer(bytes.toBuffer());
      return _indexOf(find, start, end, binary);
    }

    private int binarySearch(final T value, final Comparator<T> comparator, final int start, final int end)
    {
      int minIndex = start < 0 ? -(start + 1) : start;
      int maxIndex = end - 1;
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

    private int binarySearchRaw(final BinaryRef value, final int start, final int end)
    {
      final ByteBuffer buffer = theBuffer;
      int minIndex = start < 0 ? -(start + 1) : start;
      int maxIndex = end - 1;
      while (minIndex <= maxIndex) {
        final int medianIndex = (minIndex + maxIndex) >>> 1;
        final int offset = valueOffset(medianIndex);
        final int length = valueLength(medianIndex, offset);
        final int header = valueHeaderLength(medianIndex, length);
        final int comparison = compareTo(buffer, offset + header, length, value);
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

    private int linearSearch(final T target, final Comparator<T> comparator, final int start, final int end)
    {
      final ByteBuffer buffer = supplier.get();
      final int x = start < 0 ? -(start + 1) : start;
      int offset = valueOffset(x);
      for (int index = x; index < end; index++) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        buffer.position(offset + header);
        final int comparison = comparator.compare(strategy.fromByteBuffer(buffer, length), target);
        if (comparison < 0) {
          offset += scanDelta(index, length, header);
          continue;
        }
        return comparison == 0 ? index : -index -1;
      }
      return -(end + 1);
    }

    private int linearSearchRaw(final BinaryRef target, final int start, final int end)
    {
      final ByteBuffer buffer = theBuffer;
      final int x = start < 0 ? -(start + 1) : start;
      int offset = valueOffset(x);
      for (int index = x; index < end; index++) {
        final int length = valueLength(index, offset);
        final int header = valueHeaderLength(index, length);
        final int comparison = compareTo(buffer, offset + header, length, target);
        if (comparison < 0) {
          offset += scanDelta(index, length, header);
          continue;
        }
        return comparison == 0 ? index : -index -1;
      }
      return -(end + 1);
    }

    @Override
    public Iterator<T> iterator()
    {
      return IndexedIterable.create(this).iterator();
    }
  }

  private static int compareTo(final ByteBuffer buffer, final int offset, final int length, final BinaryRef value)
  {
    final int len1 = length;
    final int len2 = value.length();
    final int limit = Math.min(len1, len2);
    for (int i = 0; i < limit; i++) {
      final int cmp = Integer.compare(buffer.get(offset + i) & 0xff, value.get(i) & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Ints.compare(len1, len2);
  }

  private class BufferIndexedV1 extends BufferIndexed
  {
    private BufferIndexedV1(ObjectStrategy<T> strategy, Supplier<ByteBuffer> supplier) {super(strategy, supplier);}

    @Override
    protected BufferIndexed asSingleThreaded()
    {
      return new BufferIndexedV1(dedicatedStrategy(), Suppliers.ofInstance(theBuffer.asReadOnlyBuffer()));
    }

    @Override
    protected int valueLength(int index, int offset)
    {
      return theBuffer.getInt(offset);
    }

    @Override
    protected int valueHeaderLength(int index, int length)
    {
      return Integer.BYTES;
    }
  }

  private class BufferIndexedV2 extends BufferIndexed
  {
    private BufferIndexedV2(ObjectStrategy<T> strategy, Supplier<ByteBuffer> supplier) {super(strategy, supplier);}

    @Override
    protected BufferIndexed asSingleThreaded()
    {
      return new BufferIndexedV2(dedicatedStrategy(), Suppliers.ofInstance(theBuffer.asReadOnlyBuffer()));
    }

    @Override
    protected int valueLength(int index, int offset)
    {
      return VLongUtils.readUnsignedVarInt(theBuffer, offset);
    }

    @Override
    protected int valueHeaderLength(int index, int length)
    {
      return VLongUtils.sizeOfUnsignedVarInt(length);
    }
  }

  private class NoOffset extends BufferIndexedV2
  {
    private final boolean hasNull;
    private final int minLength;
    private final int maxLength;

    private NoOffset(
        boolean hasNull,
        int minLength,
        int maxLength,
        ObjectStrategy<T> strategy,
        Supplier<ByteBuffer> supplier
    )
    {
      super(strategy, supplier);
      this.hasNull = hasNull;
      this.minLength = minLength;
      this.maxLength = maxLength;
    }

    @Override
    protected Boolean hasNull()
    {
      return hasNull;
    }

    @Override
    protected BufferIndexed asSingleThreaded()
    {
      return new NoOffset(hasNull, minLength, maxLength, dedicatedStrategy(), Suppliers.ofInstance(theBuffer.asReadOnlyBuffer()));
    }

    @Override
    protected int valueOffset(int index)
    {
      return hasNull && index == 0 ? 0 : valuesOffset + (maxLength + 1) * (hasNull ? index - 1 : index);
    }

    @Override
    protected int valueLength(int index, int offset)
    {
      return hasNull && index == 0 ? 0 : minLength + theBuffer.get(offset);
    }

    @Override
    protected int valueHeaderLength(int index, int length)
    {
      return hasNull && index == 0 ? 0 : 1;
    }

    @Override
    protected int scanDelta(int index, int length, int header)
    {
      return hasNull && index == 0 ? 0 : maxLength + 1;
    }
  }

  private class Fixed extends BufferIndexedV2
  {
    private final boolean hasNull;
    private final int fixed;

    private Fixed(boolean hasNull, int fixed, ObjectStrategy<T> strategy, Supplier<ByteBuffer> supplier)
    {
      super(strategy, supplier);
      this.hasNull = hasNull;
      this.fixed = fixed;
    }

    @Override
    protected Boolean hasNull()
    {
      return hasNull;
    }

    @Override
    protected BufferIndexed asSingleThreaded()
    {
      return new Fixed(hasNull, fixed, dedicatedStrategy(), Suppliers.ofInstance(theBuffer.asReadOnlyBuffer()));
    }

    @Override
    protected int valueOffset(int index)
    {
      return hasNull && index == 0 ? 0 : valuesOffset + fixed * (hasNull ? index - 1 : index);
    }

    @Override
    protected int valueLength(int index, int offset)
    {
      return hasNull && index == 0 ? 0 : fixed;
    }

    @Override
    protected int valueHeaderLength(int index, int length)
    {
      return 0;
    }

    @Override
    protected int scanDelta(int index, int length, int header)
    {
      return hasNull && index == 0 ? 0 : fixed;
    }
  }
}
