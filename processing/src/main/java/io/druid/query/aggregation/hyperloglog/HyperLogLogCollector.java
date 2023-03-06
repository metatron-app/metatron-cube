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

package io.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.Murmur3;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.HashCollector;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Implements the HyperLogLog cardinality estimator described in:
 *
 * http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * Run this code to see a simple indication of expected errors based on different m values:
 *
 * <code>
 * for (int i = 1; i &lt; 20; ++i) {
 * System.out.printf("i[%,d], val[%,d] =&gt; error[%f%%]%n", i, 2 &lt;&lt; i, 104 / Math.sqrt(2 &lt;&lt; i));
 * }
 * </code>
 *
 * This class is *not* multi-threaded.  It can be passed among threads, but it is written with the assumption that
 * only one thread is ever calling methods on it.
 *
 * If you have multiple threads calling methods on this concurrently, I hope you manage to get correct behavior
 */
public final class HyperLogLogCollector implements Comparable<HyperLogLogCollector>, HashCollector.ScanSupport
{
  public static final String HLL_TYPE_NAME = "hyperUnique";
  public static final ValueDesc HLL_TYPE = ValueDesc.of(HLL_TYPE_NAME, HyperLogLogCollector.class);

  private static final double TWO_TO_THE_SIXTY_FOUR = Math.pow(2, 64);
  private static final double HIGH_CORRECTION_THRESHOLD = TWO_TO_THE_SIXTY_FOUR / 30.0d;
  private static final int SPARSE_BUCKET_SIZE = Byte.BYTES + Short.BYTES;

  private static final int BITS_PER_BUCKET = 4;
  private static final int RANGE = (int) Math.pow(2, BITS_PER_BUCKET) - 1;

  private static final int V_MASK = 0x07;
  private static final int B_MASK = 0x1f;

  private static final byte UPPER_NIBBLE = (byte) 0xf0;
  private static final byte LOWER_NIBBLE = (byte) 0x0f;

  /**
   * Header:
   * Byte 0: meta = B (5bit) | version (3bit, 0x01)
   * Byte 1: registerOffset
   * Byte 2-3: numNonZeroRegisters
   * Byte 4: maxOverflowValue
   * Byte 5-6: maxOverflowRegister
   */
  static final int META_BYTE = 0;
  static final int REGISTER_OFFSET_BYTE = 1;
  static final int NUM_NON_ZERO_REGISTERS_BYTE = 2;

  public static final int CONTEXT_START = 5;
  public static final int CONTEXT_END = 24;

  public HyperLogLogCollector overwrite(HyperLogLogCollector from)
  {
    if (from.isSparse()) {
      return clear().fold(from);
    } else {
      storageBuffer.order(from.storageBuffer.order()).position(0);
      storageBuffer.put(from.storageBuffer.asReadOnlyBuffer());
      storageBuffer.position(0);
      return this;
    }
  }

  // 11 : 1K, 21 : 1M
  public static enum Context
  {
    B5(5), B6(6), B7(7), B8(8), B9(9), B10(10),
    B11(11), B12(12), B13(13), B14(14), B15(15),
    B16(16), B17(17), B18(18), B19(19), B20(20),
    B21(21), B22(22), B23(23), B24(24);

    final int BITS_FOR_BUCKETS;
    final int NUM_BUCKETS;
    final int NUM_BYTES_FOR_BUCKETS;
    final int DENSE_THRESHOLD;

    final double ALPHA;

    final double LOW_CORRECTION_THRESHOLD;
    final double CORRECTION_PARAMETER;

    final int BUCKET_MASK;
    final int MIN_BYTES_REQUIRED;

    public final byte HEADER;
    public final int NUM_BYTES_FOR_DENSE_STORAGE;
    public final byte[] EMPTY_BYTES;

    final int MAX_OVERFLOW_VALUE_BYTE;
    final int MAX_OVERFLOW_REGISTER_BYTE;
    final int HEADER_NUM_BYTES;

    Context(int bits)
    {
      HEADER = (byte) ((bits << 3 | 0x01) & 0xff);
      BITS_FOR_BUCKETS = bits;
      NUM_BUCKETS = 1 << BITS_FOR_BUCKETS;
      NUM_BYTES_FOR_BUCKETS = NUM_BUCKETS >> 1;
      DENSE_THRESHOLD = NUM_BUCKETS >> 2;

      ALPHA = 0.7213 / (1 + 1.079 / NUM_BUCKETS);

      LOW_CORRECTION_THRESHOLD = (5 * NUM_BUCKETS) / 2.0d;
      CORRECTION_PARAMETER = ALPHA * NUM_BUCKETS * NUM_BUCKETS;

      BUCKET_MASK = NUM_BUCKETS - 1;
      MIN_BYTES_REQUIRED = BITS_FOR_BUCKETS - 1;

      if (bits <= 16) {
        MAX_OVERFLOW_VALUE_BYTE = 4;
        MAX_OVERFLOW_REGISTER_BYTE = 5;
        HEADER_NUM_BYTES = 7;
      } else {
        MAX_OVERFLOW_VALUE_BYTE = 5;
        MAX_OVERFLOW_REGISTER_BYTE = 6;
        HEADER_NUM_BYTES = 9;
      }
      NUM_BYTES_FOR_DENSE_STORAGE = NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES;
      EMPTY_BYTES = new byte[NUM_BYTES_FOR_DENSE_STORAGE];
      EMPTY_BYTES[0] = HEADER;
    }

    public byte[] makeEmpty()
    {
      byte[] empty = new byte[NUM_BYTES_FOR_DENSE_STORAGE];
      empty[0] = HEADER;
      return empty;
    }
  }

  public static final Context DEFAULT_CTX = Context.B11;

  private static final double[] MIN_NUM_REGISTER_LOOKUP = new double[64 + 1]; // handle zero

  static {
    for (int position = 0; position < MIN_NUM_REGISTER_LOOKUP.length; ++position) {
      MIN_NUM_REGISTER_LOOKUP[position] = 1.0d / Math.pow(2, position);
    }
  }

  public static HyperLogLogCollector makeLatestCollector()
  {
    return makeLatestCollector(DEFAULT_CTX);
  }

  public static HyperLogLogCollector makeLatestCollector(int b)
  {
    return makeLatestCollector(getContext(b));
  }

  public static HyperLogLogCollector makeLatestCollector(Context context)
  {
    return new HyperLogLogCollector(context);
  }

  public static Object deserialize(Object object)
  {
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(StringUtils.decodeBase64((String) object));
    } else {
      return object;
    }

    return from(buffer);
  }

  /**
   * Create a wrapper object around an HLL sketch contained within a buffer. The position and limit of
   * the buffer may be changed; if you do not want this to happen, you can duplicate the buffer before
   * passing it in.
   *
   * The mark and byte order of the buffer will not be modified.
   *
   * @param buffer buffer containing an HLL sketch starting at its position and ending at its limit
   *
   * @return HLLC wrapper object
   */
  public static HyperLogLogCollector from(ByteBuffer buffer)
  {
    return new HyperLogLogCollector(buffer);
  }

  public static HyperLogLogCollector from(Context context, ByteBuffer buffer, int position)
  {
    buffer.limit(position + context.NUM_BYTES_FOR_DENSE_STORAGE);
    buffer.position(position);
    ByteBuffer slice = buffer.slice();
    buffer.clear();
    return new HyperLogLogCollector(context, slice);
  }

  public static HyperLogLogCollector copy(Context context, ByteBuffer buf, int position)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.from(context, buf, position);
    return new HyperLogLogCollector(context, ByteBuffer.wrap(collector.toByteArray()));
  }

  public static Context getContext(int b)
  {
    return b == 0 ? DEFAULT_CTX : Context.values()[b - CONTEXT_START];
  }

  private double applyCorrection(double e, int zeroCount)
  {
    e = context.CORRECTION_PARAMETER / e;

    if (e < context.LOW_CORRECTION_THRESHOLD) {
      return zeroCount == 0 ? e : context.NUM_BUCKETS * Math.log(context.NUM_BUCKETS / (double) zeroCount);
    }

    if (e > HIGH_CORRECTION_THRESHOLD) {
      final double ratio = e / TWO_TO_THE_SIXTY_FOUR;
      if (ratio >= 1) {
        // handle very unlikely case that value is > 2^64
        return Double.MAX_VALUE;
      } else {
        return -TWO_TO_THE_SIXTY_FOUR * Math.log(1 - ratio);
      }
    }

    return e;
  }

  public static double estimateByteBuffer(ByteBuffer buf)
  {
    return from(buf.duplicate()).estimateCardinality();
  }

  // minNum == 0, generally
  private double estimateSparse(
      final ByteBuffer buf,
      final byte minNum,
      final byte overflowValue,
      final int overflowPosition,
      final boolean isUpperNibble
  )
  {
    final ByteBuffer copy = buf.asReadOnlyBuffer();
    double e = 0.0d;
    int zeroCount = context.NUM_BUCKETS - 2 * (buf.remaining() / 3);
    while (copy.hasRemaining()) {
      final int position = getUnsignedShort(copy);
      final int register = (int) copy.get() & 0xff;
      int upperNibble = ((register & UPPER_NIBBLE) >>> BITS_PER_BUCKET) + minNum;
      int lowerNibble = (register & LOWER_NIBBLE) + minNum;
      if (overflowValue != 0 && position == overflowPosition) {
        if (isUpperNibble) {
          upperNibble = Math.max(upperNibble, overflowValue);
        } else {
          lowerNibble = Math.max(lowerNibble, overflowValue);
        }
      }
      zeroCount += upperNibble == 0 ? 1 : 0;
      zeroCount += lowerNibble == 0 ? 1 : 0;
      e += MIN_NUM_REGISTER_LOOKUP[upperNibble];
      e += MIN_NUM_REGISTER_LOOKUP[lowerNibble];
    }

    e += zeroCount;
    return applyCorrection(e, zeroCount);
  }

  private double estimateDense(
      final ByteBuffer buf,
      final byte minNum,
      final byte overflowValue,
      final int overflowPosition,
      final boolean isUpperNibble
  )
  {
    final ByteBuffer copy = buf.asReadOnlyBuffer();
    double e = 0.0d;
    int zeroCount = 0;
    int position = 0;
    while (copy.hasRemaining()) {
      final int register = (int) copy.get() & 0xff;
      if (register == 0 && minNum == 0) {
        e += MIN_NUM_REGISTER_LOOKUP[0] * 2;
        zeroCount += 2;
      } else {
        int upperNibble = ((register & UPPER_NIBBLE) >>> BITS_PER_BUCKET) + minNum;
        int lowerNibble = (register & LOWER_NIBBLE) + minNum;
        if (overflowValue != 0 && position == overflowPosition) {
          if (isUpperNibble) {
            upperNibble = Math.max(upperNibble, overflowValue);
          } else {
            lowerNibble = Math.max(lowerNibble, overflowValue);
          }
        }
        zeroCount += upperNibble == 0 ? 1 : 0;
        zeroCount += lowerNibble == 0 ? 1 : 0;
        e += MIN_NUM_REGISTER_LOOKUP[upperNibble];
        e += MIN_NUM_REGISTER_LOOKUP[lowerNibble];
      }
      position++;
    }

    return applyCorrection(e, zeroCount);
  }

  public double[] _estimateMinMax()
  {
    final byte r = getRegisterOffset();
    final int nz = getNumNonZeroRegisters();
    final byte ov = getMaxOverflowValue();

    final int x = ov == 0 ? nz : nz - 1;
    final int y = context.NUM_BUCKETS - nz;
    double e1 = 1.0d / Math.pow(2, r + 1) * x + 1.0d / Math.pow(2, r) * y;
    double e2 = 1.0d / Math.pow(2, r + 0xf) * x + 1.0d / Math.pow(2, r) * y;
    if (ov > 0) {
      e1 += 1.0d / Math.pow(2, Math.max(ov, r + 1));
      e2 += 1.0d / Math.pow(2, Math.max(ov, r + 0x0f));
    }
    return new double[] {applyCorrection(e1, y), applyCorrection(e2, y)};
  }

  /**
   * Checks if the payload for the given ByteBuffer is sparse or not.
   * The given buffer must be positioned at getPayloadBytePosition() prior to calling isSparse
   */
  private static boolean isSparse(ByteBuffer buffer, Context context)
  {
    return buffer.remaining() != context.NUM_BYTES_FOR_BUCKETS;
  }

  private final Context context;
  private ByteBuffer storageBuffer;
  private Double estimatedCardinality;

  private HyperLogLogCollector(Context context)
  {
    this.context = context;
    this.storageBuffer = ByteBuffer.wrap(context.makeEmpty());
  }

  private HyperLogLogCollector(ByteBuffer byteBuffer)
  {
    if (byteBuffer.position() != 0) {
      byteBuffer = byteBuffer.slice();
    }
    final byte meta = byteBuffer.get(META_BYTE);
    Preconditions.checkArgument((meta & V_MASK) == 0x01);
    this.context = getContext((meta >> 3) & B_MASK);
    this.storageBuffer = byteBuffer;
  }

  private HyperLogLogCollector(Context context, ByteBuffer byteBuffer)
  {
    this.context = context;
    this.storageBuffer = byteBuffer;
  }

  public HyperLogLogCollector clear()
  {
    estimatedCardinality = null;
    if (storageBuffer.hasArray()) {
      Arrays.fill(storageBuffer.array(), (byte) 0);
      storageBuffer.array()[0] = context.HEADER;
    } else {
      storageBuffer.position(0);
      storageBuffer.put(context.EMPTY_BYTES);
    }
    storageBuffer.position(0);
    return this;
  }

  public boolean isSparse()
  {
    return storageBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE;
  }

  public int numSparseRegister()
  {
    return (storageBuffer.remaining() - context.HEADER_NUM_BYTES) / 3;
  }

  public byte getRegisterOffset()
  {
    return storageBuffer.get(REGISTER_OFFSET_BYTE);
  }

  private void setRegisterOffset(byte registerOffset)
  {
    storageBuffer.put(REGISTER_OFFSET_BYTE, registerOffset);
  }

  public int getNumNonZeroRegisters()
  {
    if (context.BITS_FOR_BUCKETS <= 16) {
      return getUnsignedShort(storageBuffer, NUM_NON_ZERO_REGISTERS_BYTE);
    }
    return get24ByteInteger(NUM_NON_ZERO_REGISTERS_BYTE);
  }

  private void setNumNonZeroRegisters(int numNonZeroRegisters)
  {
    if (context.BITS_FOR_BUCKETS <= 16) {
      putUnsignedShort(storageBuffer, NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
    } else {
      set24ByteInteger(NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
    }
  }

  public byte getMaxOverflowValue()
  {
    return storageBuffer.get(context.MAX_OVERFLOW_VALUE_BYTE);
  }

  private void setMaxOverflowValue(byte value)
  {
    storageBuffer.put(context.MAX_OVERFLOW_VALUE_BYTE, value);
  }

  public int getMaxOverflowRegister()
  {
    if (context.BITS_FOR_BUCKETS <= 16) {
      return getUnsignedShort(storageBuffer, context.MAX_OVERFLOW_REGISTER_BYTE);
    }
    return get24ByteInteger(context.MAX_OVERFLOW_REGISTER_BYTE);
  }

  private void setMaxOverflowRegister(int register)
  {
    if (context.BITS_FOR_BUCKETS <= 16) {
      putUnsignedShort(storageBuffer, context.MAX_OVERFLOW_REGISTER_BYTE, register);
    } else {
      set24ByteInteger(context.MAX_OVERFLOW_REGISTER_BYTE, register);
    }
  }

  private int get24ByteInteger(int offset)
  {
    int register = storageBuffer.get(offset) & 0xff;
    register = (register << 8) + (storageBuffer.get(offset + 1) & 0xff);
    register = (register << 8) + (storageBuffer.get(offset + 2) & 0xff);
    return register;
  }

  private void set24ByteInteger(int offset, int value)
  {
    storageBuffer.put(offset, (byte)(value >> 16 & 0xff));
    storageBuffer.put(offset + 1, (byte)(value >> 8 & 0xff));
    storageBuffer.put(offset + 2, (byte)(value & 0xff));
  }

  public void add(final byte[] hashedValue)
  {
    if (hashedValue.length < context.MIN_BYTES_REQUIRED) {
      throw new IAE("Insufficient bytes, need[%d] got [%d]", context.MIN_BYTES_REQUIRED, hashedValue.length);
    }

    final ByteBuffer buffer = ByteBuffer.wrap(hashedValue);

    int bucket = getUnsignedShort(buffer, hashedValue.length - 2) & context.BUCKET_MASK;

    byte positionOf1 = 0;
    for (int i = 0; i < 8; ++i) {
      final byte lookupVal = ByteBitLookup.lookup[UnsignedBytes.toInt(hashedValue[i])];
      if (lookupVal != 0) {
        positionOf1 += lookupVal;
        break;
      }
      positionOf1 += (byte) 8;
    }

    addOnBucket(bucket, positionOf1);
  }

  // murmur128
  public void add(final long[] hashedValue)
  {
    final int bucket = (int) (hashedValue[1] & context.BUCKET_MASK);

    byte positionOf1 = 0;
    long reversed = Long.reverseBytes(hashedValue[0]);
    for (int i = 0; i < 8; ++i) {
      final byte lookupVal = ByteBitLookup.lookup[(int) (reversed & 0xff)];
      if (lookupVal != 0) {
        positionOf1 += lookupVal;
        break;
      }
      positionOf1 += (byte) 8;
      reversed >>= 8;
    }

    addOnBucket(bucket, positionOf1);
  }

  // use high bits for bucket. Murmur3 seemed coliding frequently in lower 32 bits
  public void add(final long hashedValue)
  {
    final int bucket = (int) (hashedValue >>> -context.BITS_FOR_BUCKETS);
    // ~23 for 100M, ~32 for 1B
    final byte positionOf1 = (byte) (Long.numberOfLeadingZeros(hashedValue << context.BITS_FOR_BUCKETS) + 1);
    addOnBucket(bucket, positionOf1);
  }

  @VisibleForTesting
  public synchronized void addOnBucket(int bucket, byte positionOf1)
  {
    estimatedCardinality = null;

    if (storageBuffer.isReadOnly()) {
      convertToMutableByteBuffer();
    }

    _addOnBucket(bucket, positionOf1);
  }

  private void _addOnBucket(final int bucket, final byte positionOf1)
  {
    byte registerOffset = getRegisterOffset();
    // discard everything outside the range we care about
    if (positionOf1 <= registerOffset) {
      return;
    }

    if (positionOf1 > registerOffset + RANGE) {
      final byte currMax = getMaxOverflowValue();
      if (positionOf1 > currMax) {
        if (currMax > 0 && currMax <= registerOffset + RANGE) {
          // this could be optimized by having an add without sanity checks
          _addOnBucket(getMaxOverflowRegister(), currMax);
        }
        setMaxOverflowValue(positionOf1);
        setMaxOverflowRegister(bucket);
      }
    } else {
      // whatever value we add must be stored in 4 bits
      final int numNonZeroRegisters = addNibbleRegister(bucket, (byte) ((0xff & positionOf1) - registerOffset));
      if (numNonZeroRegisters == context.NUM_BUCKETS) {
        setRegisterOffset(++registerOffset);
        setNumNonZeroRegisters(decrementBuckets());
      }
    }
  }

  public synchronized HyperLogLogCollector fold(HyperLogLogCollector other)
  {
    if (other == null || other.storageBuffer.remaining() == 0) {
      return this;
    }
    Preconditions.checkArgument(
        context.HEADER == other.context.HEADER, "Version mismatch %d : %d", context.HEADER, other.context.HEADER
    );

    if (storageBuffer.isReadOnly()) {
      convertToMutableByteBuffer();
    }

    if (storageBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE) {
      convertToDenseStorage();
    }

    estimatedCardinality = null;

    if (getRegisterOffset() < other.getRegisterOffset()) {
      // "Swap" the buffers so that we are folding into the one with the higher offset
      final ByteBuffer tmpBuffer = ByteBuffer.allocate(storageBuffer.remaining());
      tmpBuffer.put(storageBuffer.asReadOnlyBuffer());
      tmpBuffer.clear();

      storageBuffer.duplicate().put(other.storageBuffer.asReadOnlyBuffer());

      other = HyperLogLogCollector.from(tmpBuffer);
    }

    final ByteBuffer otherBuffer = other.storageBuffer;

    // Save position and restore later to avoid allocations due to duplicating the otherBuffer object.
    final int otherPosition = otherBuffer.position();

    try {
      final byte otherOffset = other.getRegisterOffset();

      byte myOffset = getRegisterOffset();
      int numNonZero = getNumNonZeroRegisters();

      final int offsetDiff = myOffset - otherOffset;
      if (offsetDiff < 0) {
        throw new ISE("offsetDiff[%d] < 0, shouldn't happen because of swap.", offsetDiff);
      }

      otherBuffer.position(context.HEADER_NUM_BYTES);

      if (isSparse(otherBuffer, context)) {
        while (otherBuffer.hasRemaining()) {
          final int position = getUnsignedShort(otherBuffer);
          final byte value = otherBuffer.get();
          numNonZero += mergeAndStoreByteRegister(storageBuffer, position, offsetDiff, value);
        }
      } else { // dense
        int position = context.HEADER_NUM_BYTES;
        while (otherBuffer.hasRemaining()) {
          final byte value = otherBuffer.get();
          numNonZero += mergeAndStoreByteRegister(storageBuffer, position, offsetDiff, value);
          position++;
        }
      }
      if (numNonZero == context.NUM_BUCKETS) {
        numNonZero = decrementBuckets();
        setRegisterOffset(++myOffset);
        setNumNonZeroRegisters(numNonZero);
      }

      // no need to call setRegisterOffset(myOffset) here, since it gets updated every time myOffset is incremented
      setNumNonZeroRegisters(numNonZero);

      // this will add the max overflow and also recheck if offset needs to be shifted
      addOnBucket(other.getMaxOverflowRegister(), other.getMaxOverflowValue());

      return this;
    }
    finally {
      otherBuffer.position(otherPosition);
    }
  }

  public HyperLogLogCollector fold(ByteBuffer buffer)
  {
    return fold(from(buffer.duplicate()));
  }

  @VisibleForTesting
  ByteBuffer toByteBuffer()
  {
    return ByteBuffer.wrap(toByteArray());
  }

  @JsonValue
  public byte[] toByteArray()
  {
    final int numNonZeroRegisters = getNumNonZeroRegisters();
    if (numNonZeroRegisters < context.DENSE_THRESHOLD && storageBuffer.remaining() == context.NUM_BYTES_FOR_DENSE_STORAGE) {
      return writeDenseAsSparse(numNonZeroRegisters);
    } else {
      return copyToArray();
    }
  }

  private static final ThreadLocal<BytesOutputStream> SCRATCH = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  // smaller footprint
  private byte[] writeDenseAsSparse(final int numNonZeroRegisters)
  {
    final BytesOutputStream scratch = SCRATCH.get();
    scratch.clear();
    scratch.ensureCapacity(context.HEADER_NUM_BYTES + numNonZeroRegisters * SPARSE_BUCKET_SIZE);

    final byte[] output = scratch.unwrap();
    storageBuffer.get(output, 0, context.HEADER_NUM_BYTES);
    if (context.BITS_FOR_BUCKETS <= 16 && ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(output, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(output, context.MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
    }

    int remain = numNonZeroRegisters;
    int x = context.HEADER_NUM_BYTES;
    if (storageBuffer.hasArray()) {
      final byte[] array = storageBuffer.array();
      final int offset = storageBuffer.arrayOffset();
      for (int i = context.HEADER_NUM_BYTES; remain > 0 && i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
        final byte v = array[offset + i];
        if (v != 0) {
          // same as writeShort() but seemed not inlined
          output[x] = (byte) (i >>> Byte.SIZE & 0xff);
          output[x + 1] = (byte) (i & 0xff);
          output[x + 2] = v;
          x += SPARSE_BUCKET_SIZE;
          remain -= nibbles(v);
        }
      }
    } else {
      for (int i = context.HEADER_NUM_BYTES; remain > 0 && i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
        final byte v = storageBuffer.get(i);
        if (v != 0) {
          // same as writeShort() but seemed not inlined
          output[x] = (byte) (i >>> Byte.SIZE & 0xff);
          output[x + 1] = (byte) (i & 0xff);
          output[x + 2] = v;
          x += SPARSE_BUCKET_SIZE;
          remain -= nibbles(v);
        }
      }
    }
    storageBuffer.position(0);
    return Arrays.copyOfRange(output, 0, x);
  }

  private byte[] copyToArray()
  {
    if (storageBuffer.hasArray()) {
      final byte[] array = storageBuffer.array();
      final int offset = storageBuffer.arrayOffset();
      return Arrays.copyOfRange(array, offset + storageBuffer.position(), offset + storageBuffer.limit());
    }
    final byte[] array = new byte[storageBuffer.remaining()];
    storageBuffer.get(array);
    storageBuffer.position(0);
    if (context.BITS_FOR_BUCKETS <= 16 && ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(array, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(array, context.MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
    }
    return array;
  }

  // big-endian
  private static void writeShort(final byte[] target, final int offset, final int value)
  {
    target[offset] = (byte) (value >>> Byte.SIZE & 0xff);
    target[offset + 1] = (byte) (value & 0xff);
  }

  public long estimateCardinalityRound()
  {
    return Math.round(estimateCardinality());
  }

  public double estimateCardinality()
  {
    if (estimatedCardinality == null) {
      byte registerOffset = getRegisterOffset();
      byte overflowValue = getMaxOverflowValue();
      int overflowRegister = getMaxOverflowRegister();
      int overflowPosition = overflowRegister >>> 1;
      boolean isUpperNibble = ((overflowRegister & 0x1) == 0);

      storageBuffer.position(context.HEADER_NUM_BYTES);

      if (isSparse(storageBuffer, context)) {
        estimatedCardinality = estimateSparse(
            storageBuffer,
            registerOffset,
            overflowValue,
            overflowPosition,
            isUpperNibble
        );
      } else {
        estimatedCardinality = estimateDense(
            storageBuffer,
            registerOffset,
            overflowValue,
            overflowPosition,
            isUpperNibble
        );
      }

      storageBuffer.position(0);
    }
    return estimatedCardinality;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByteBuffer otherBuffer = ((HyperLogLogCollector) o).storageBuffer;

    if (storageBuffer == null && otherBuffer != null) {
      return false;
    }

    if (storageBuffer == null && otherBuffer == null) {
      return true;
    }

    final ByteBuffer denseStorageBuffer;
    if (storageBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE) {
      HyperLogLogCollector denseCollector = HyperLogLogCollector.from(storageBuffer.duplicate());
      denseCollector.convertToDenseStorage();
      denseStorageBuffer = denseCollector.storageBuffer;
    } else {
      denseStorageBuffer = storageBuffer;
    }

    if (otherBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE) {
      HyperLogLogCollector otherCollector = HyperLogLogCollector.from(otherBuffer.duplicate());
      otherCollector.convertToDenseStorage();
      otherBuffer = otherCollector.storageBuffer;
    }

    return denseStorageBuffer.equals(otherBuffer);
  }

  @Override
  public String toString()
  {
    return "HyperLogLogCollector{" +
           "context=" + context +
           ", registerOffset=" + getRegisterOffset() +
           ", numNonZeroRegisters=" + getNumNonZeroRegisters() +
           ", maxOverflowValue=" + getMaxOverflowValue() +
           ", maxOverflowRegister=" + getMaxOverflowRegister() +
           '}';
  }

  private int decrementBuckets()
  {
    int count = 0;
    for (int i = context.HEADER_NUM_BYTES; i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
      final byte val = (byte) (storageBuffer.get(i) - 0x11);
      storageBuffer.put(i, val);
      count += nibbles(val);
    }
    return count;
  }

  private static int nibbles(final byte v)
  {
    return ((v & UPPER_NIBBLE) == 0 ? 0 : 1) + ((v & LOWER_NIBBLE) == 0 ? 0 : 1);
  }

  private void convertToMutableByteBuffer()
  {
    ByteBuffer tmpBuffer = ByteBuffer.allocate(storageBuffer.remaining());
    tmpBuffer.order(storageBuffer.order());
    tmpBuffer.put(storageBuffer.asReadOnlyBuffer());
    tmpBuffer.position(0);
    storageBuffer = tmpBuffer;
  }

  private void convertToDenseStorage()
  {
    final byte[] array = new byte[context.NUM_BYTES_FOR_DENSE_STORAGE];
    storageBuffer.get(array, 0, context.HEADER_NUM_BYTES);
    if (context.BITS_FOR_BUCKETS <= 16 && ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(array, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(array, context.MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
    }

    final ByteBuffer tmpBuffer = ByteBuffer.wrap(array);

    tmpBuffer.position(context.HEADER_NUM_BYTES);
    // put payload
    while (storageBuffer.hasRemaining()) {
      tmpBuffer.put(getUnsignedShort(storageBuffer), storageBuffer.get());
    }
    tmpBuffer.rewind();
    storageBuffer = tmpBuffer;
  }

  private int addNibbleRegister(final int bucket, final byte positionOf1)
  {
    final int position = context.HEADER_NUM_BYTES + (bucket >> 1);
    final boolean isUpperNibble = (bucket & 0x1) == 0;
    final byte shiftedPositionOf1 = isUpperNibble ? (byte) (positionOf1 << BITS_PER_BUCKET) : positionOf1;

    if (storageBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE) {
      convertToDenseStorage();
    }

    final byte origVal = storageBuffer.get(position);

    final byte target;
    final byte remain;
    if (isUpperNibble) {
      target = (byte) (origVal & UPPER_NIBBLE);
      remain = (byte) (origVal & LOWER_NIBBLE);
    } else {
      target = (byte) (origVal & LOWER_NIBBLE);
      remain = (byte) (origVal & UPPER_NIBBLE);
    }

    int numNonZeroRegs = -1;  // -1 : not changed
    // if something was at zero, we have to increase the numNonZeroRegisters
    if (target == 0 && shiftedPositionOf1 != 0) {
      numNonZeroRegs = getNumNonZeroRegisters() + 1;
      setNumNonZeroRegisters(numNonZeroRegs);
    }

    final byte max = UnsignedBytes.max(target, shiftedPositionOf1);
    if (target != max) {
      storageBuffer.put(position, (byte) (max | remain));
    }

    return numNonZeroRegs;
  }

  /**
   * Returns the number of registers that are no longer zero after the value was added
   *
   * @param position   The position into the byte buffer, this position represents two "registers"
   * @param offsetDiff The difference in offset between the byteToAdd and the current HyperLogLogCollector
   * @param byteToAdd  The byte to merge into the current HyperLogLogCollector
   */
  private static short mergeAndStoreByteRegister(
      final ByteBuffer storageBuffer,
      final int position,
      final int offsetDiff,
      final byte byteToAdd
  )
  {
    if (byteToAdd == 0) {
      return 0;
    }

    final byte currVal = storageBuffer.get(position);
    if (currVal == byteToAdd) {
      return 0;
    }

    final int upperNibble = currVal & 0xf0;
    final int lowerNibble = currVal & 0x0f;

    // subtract the differences so that the nibbles align
    final int otherUpper = (byteToAdd & 0xf0) - (offsetDiff << BITS_PER_BUCKET);
    final int otherLower = (byteToAdd & 0x0f) - offsetDiff;

    final int newUpper = Math.max(upperNibble, otherUpper);
    final int newLower = Math.max(lowerNibble, otherLower);

    final byte newValue = (byte) ((newUpper | newLower) & 0xff);
    if (currVal == newValue) {
      return 0;
    }

    storageBuffer.put(position, newValue);

    short numNoLongerZero = 0;
    if (upperNibble == 0 && newUpper > 0) {
      ++numNoLongerZero;
    }
    if (lowerNibble == 0 && newLower > 0) {
      ++numNoLongerZero;
    }

    return numNoLongerZero;
  }

  @Override
  public int compareTo(HyperLogLogCollector other)
  {
    return Double.compare(estimateCardinality(), other.estimateCardinality());
  }

  @Override
  public void collect(BytesRef[] values, BytesRef bytes)
  {
    add(Murmur3.hash64(bytes));
  }

  @Override
  public void collect(DimensionSelector.Scannable scannable)
  {
    scannable.scan((ix, buffer, offset, length) -> add(Murmur3.hash64(buffer, offset, length)));
  }

  public static int getUnsignedShort(ByteBuffer bb)
  {
    return bb.getShort() & 0xffff;
  }

  public static void putUnsignedShort(ByteBuffer bb, int value)
  {
    bb.putShort((short) (value & 0xffff));
  }

  public static int getUnsignedShort(ByteBuffer bb, int position)
  {
    return bb.getShort(position) & 0xffff;
  }

  public static void putUnsignedShort(ByteBuffer bb, int position, int value)
  {
    bb.putShort(position, (short) (value & 0xffff));
  }

  private static class Cache
  {
    private static final int BUCKET_BIT = 24;
    private static final int VALUE_BIT = 8;

    private static final int BUCKET_MASK = 0xffffff00;
    private static final int VALUE_MASK = 0xff;

    private int index;
    private final int[] cache;

    private Cache(int length) {this.cache = new int[length];}

    public boolean fold(HyperLogLogCollector collector, HyperLogLogCollector fail)
    {
      Preconditions.checkArgument(collector.isSparse());
      if (index + collector.numSparseRegister() > cache.length) {
        fail.fold(collector);
        for (int i = 0; i < index; i++) {
          final int mask = cache[i] >> VALUE_BIT;
          final byte pos1 = (byte) (cache[i] & VALUE_MASK);
          fail._addOnBucket(mask, pos1);
        }
        return false;
      }
      final ByteBuffer copy = collector.storageBuffer.asReadOnlyBuffer();
      copy.position(collector.context.HEADER_NUM_BYTES);
out:
      while (copy.hasRemaining()) {
        final int position = getUnsignedShort(copy) << VALUE_BIT;
        final int value = (int) copy.get() & 0xff;
        for (int i = 0; i < index; i++) {
          if ((cache[i] & BUCKET_MASK) == position) {
            if (value > (cache[i] & VALUE_MASK)) {
              cache[i] = position | value;
            }
            continue out;
          }
        }
        cache[index++] = position | value;
      }
      return true;
    }

    public void clear()
    {
      index = 0;
    }
  }
}
