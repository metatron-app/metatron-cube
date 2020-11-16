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
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.HashCollector;
import io.druid.query.aggregation.Murmur3;

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
public final class HyperLogLogCollector implements Comparable<HyperLogLogCollector>, HashCollector
{
  public static final String HLL_TYPE_NAME = "hyperUnique";
  public static final ValueDesc HLL_TYPE = ValueDesc.of(HLL_TYPE_NAME, HyperLogLogCollector.class);

  /**
   * Header:
   * Byte 0: version  (revised to b param. 11 -> 1, 12 -> 2, ..., 16 -> 6)
   * Byte 1: registerOffset
   * Byte 2-3: numNonZeroRegisters
   * Byte 4: maxOverflowValue
   * Byte 5-6: maxOverflowRegister
   */
  static final int VERSION_BYTE = 0;
  static final int REGISTER_OFFSET_BYTE = 1;
  static final int NUM_NON_ZERO_REGISTERS_BYTE = 2;
  static final int MAX_OVERFLOW_VALUE_BYTE = 4;
  static final int MAX_OVERFLOW_REGISTER_BYTE = 5;
  static final int HEADER_NUM_BYTES = 7;

  static final int CONTEXT_START = 11;
  static final int VERSION_TO_B = 10;

  public static enum Context
  {
    B11(11), B12(12), B13(13), B14(14), B15(15), B16(16);

    final int BITS_FOR_BUCKETS;
    final int NUM_BUCKETS;
    final int NUM_BYTES_FOR_BUCKETS;

    final double ALPHA;

    final double LOW_CORRECTION_THRESHOLD;
    final double HIGH_CORRECTION_THRESHOLD;
    final double CORRECTION_PARAMETER;

    final int BUCKET_MASK;
    final int MIN_BYTES_REQUIRED;

    public final byte VERSION;
    public final int NUM_BYTES_FOR_DENSE_STORAGE;
    public final byte[] EMPTY_BYTES;

    Context(int bits)
    {
      VERSION = (byte) (bits - VERSION_TO_B);
      BITS_FOR_BUCKETS = bits;
      NUM_BUCKETS = 1 << BITS_FOR_BUCKETS;
      NUM_BYTES_FOR_BUCKETS = NUM_BUCKETS / 2;

      ALPHA = 0.7213 / (1 + 1.079 / NUM_BUCKETS);

      LOW_CORRECTION_THRESHOLD = (5 * NUM_BUCKETS) / 2.0d;
      HIGH_CORRECTION_THRESHOLD = TWO_TO_THE_SIXTY_FOUR / 30.0d;
      CORRECTION_PARAMETER = ALPHA * NUM_BUCKETS * NUM_BUCKETS;

      BUCKET_MASK = NUM_BUCKETS - 1;
      MIN_BYTES_REQUIRED = BITS_FOR_BUCKETS - 1;

      NUM_BYTES_FOR_DENSE_STORAGE = NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES;
      EMPTY_BYTES = new byte[NUM_BYTES_FOR_DENSE_STORAGE];
      EMPTY_BYTES[0] = VERSION;
    }

    public byte[] makeEmpty()
    {
      byte[] empty = new byte[NUM_BYTES_FOR_DENSE_STORAGE];
      empty[0] = VERSION;
      return empty;
    }
  }

  public static final Context DEFAULT_CTX = Context.B11;

  private static final int DENSE_THRESHOLD = 256;
  private static final int SPARSE_BUCKET_SIZE = Byte.BYTES + Short.BYTES;

  private static final double TWO_TO_THE_SIXTY_FOUR = Math.pow(2, 64);
  private static final int BITS_PER_BUCKET = 4;
  private static final int RANGE = (int) Math.pow(2, BITS_PER_BUCKET) - 1;

  private static final double[][] MIN_NUM_REGISTER_LOOKUP = new double[64][256];

  // we have to keep track of the number of zeroes in each of the two halves of the byte register (0, 1, or 2)
  private static final int[] NUM_ZERO_LOOKUP = new int[256];

  static {
    for (int registerOffset = 0; registerOffset < 64; ++registerOffset) {
      for (int register = 0; register < 256; ++register) {
        final int upper = ((register & 0xf0) >> 4) + registerOffset;
        final int lower = (register & 0x0f) + registerOffset;
        MIN_NUM_REGISTER_LOOKUP[registerOffset][register] = 1.0d / Math.pow(2, upper) + 1.0d / Math.pow(2, lower);
      }
    }
    for (int i = 0; i < NUM_ZERO_LOOKUP.length; ++i) {
      NUM_ZERO_LOOKUP[i] = ((i & 0xf0) == 0 ? 1 : 0) + ((i & 0x0f) == 0 ? 1 : 0);
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

  public static HyperLogLogCollector from(ByteBuffer buffer, int position)
  {
    Context context = getContext(buffer.get(position) + VERSION_TO_B);
    buffer.limit(position + context.NUM_BYTES_FOR_DENSE_STORAGE);
    buffer.position(position);
    ByteBuffer slice = buffer.slice();
    buffer.clear();
    return new HyperLogLogCollector(context, slice);
  }

  public static HyperLogLogCollector copy(ByteBuffer buf, int position)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.from(buf, position);
    return HyperLogLogCollector.from(ByteBuffer.wrap(collector.toByteArray()));
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

    if (e > context.HIGH_CORRECTION_THRESHOLD) {
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
      if (overflowValue != 0 && position == overflowPosition) {
        int upperNibble = ((register & 0xf0) >>> BITS_PER_BUCKET) + minNum;
        int lowerNibble = (register & 0x0f) + minNum;
        if (isUpperNibble) {
          upperNibble = Math.max(upperNibble, overflowValue);
        } else {
          lowerNibble = Math.max(lowerNibble, overflowValue);
        }
        e += 1.0d / Math.pow(2, upperNibble) + 1.0d / Math.pow(2, lowerNibble);
        zeroCount += (((upperNibble & 0xf0) == 0) ? 1 : 0) + (((lowerNibble & 0x0f) == 0) ? 1 : 0);
      } else {
        e += MIN_NUM_REGISTER_LOOKUP[minNum][register];
        zeroCount += NUM_ZERO_LOOKUP[register];
      }
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
      if (overflowValue != 0 && position == overflowPosition) {
        int upperNibble = ((register & 0xf0) >>> BITS_PER_BUCKET) + minNum;
        int lowerNibble = (register & 0x0f) + minNum;
        if (isUpperNibble) {
          upperNibble = Math.max(upperNibble, overflowValue);
        } else {
          lowerNibble = Math.max(lowerNibble, overflowValue);
        }
        e += 1.0d / Math.pow(2, upperNibble) + 1.0d / Math.pow(2, lowerNibble);
        zeroCount += (((upperNibble & 0xf0) == 0) ? 1 : 0) + (((lowerNibble & 0x0f) == 0) ? 1 : 0);
      } else {
        e += MIN_NUM_REGISTER_LOOKUP[minNum][register];
        zeroCount += NUM_ZERO_LOOKUP[register];
      }
      position++;
    }

    return applyCorrection(e, zeroCount);
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
    this.storageBuffer = ByteBuffer.wrap(new byte[]{context.VERSION, 0, 0, 0, 0, 0, 0})
                                   .asReadOnlyBuffer();
  }

  private HyperLogLogCollector(ByteBuffer byteBuffer)
  {
    if (byteBuffer.position() != 0) {
      byteBuffer = byteBuffer.slice();
    }
    this.context = getContext(byteBuffer.get(0) + VERSION_TO_B);
    this.storageBuffer = byteBuffer;
  }

  private HyperLogLogCollector(Context context, ByteBuffer byteBuffer)
  {
    if (byteBuffer.position() != 0) {
      byteBuffer = byteBuffer.slice();
    }
    this.context = context;
    this.storageBuffer = byteBuffer;
  }

  public Context getContext()
  {
    return context;
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
    return getUnsignedShort(storageBuffer, NUM_NON_ZERO_REGISTERS_BYTE);
  }

  private void setNumNonZeroRegisters(int numNonZeroRegisters)
  {
    putUnsignedShort(storageBuffer, NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
  }

  public byte getMaxOverflowValue()
  {
    return storageBuffer.get(MAX_OVERFLOW_VALUE_BYTE);
  }

  private void setMaxOverflowValue(byte value)
  {
    storageBuffer.put(MAX_OVERFLOW_VALUE_BYTE, value);
  }

  public int getMaxOverflowRegister()
  {
    return getUnsignedShort(storageBuffer, MAX_OVERFLOW_REGISTER_BYTE);
  }

  private void setMaxOverflowRegister(int register)
  {
    putUnsignedShort(storageBuffer, MAX_OVERFLOW_REGISTER_BYTE, register);
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
      byte lookupVal = ByteBitLookup.lookup[UnsignedBytes.toInt(hashedValue[i])];
      switch (lookupVal) {
        case 0:
          positionOf1 += (byte) 8;
          continue;
        default:
          positionOf1 += lookupVal;
          i = 8;
          break;
      }
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
    // discard everything outside of the range we care about
    if (positionOf1 <= registerOffset) {
      return;
    }

    if (positionOf1 > registerOffset + RANGE) {
      final byte currMax = getMaxOverflowValue();
      if (positionOf1 > currMax) {
        if (currMax <= registerOffset + RANGE) {
          // this could be optimized by having an add without sanity checks
          _addOnBucket(getMaxOverflowRegister(), currMax);
        }
        setMaxOverflowValue(positionOf1);
        setMaxOverflowRegister(bucket);
      }
    } else {
      // whatever value we add must be stored in 4 bits
      final int numNonZeroRegisters = addNibbleRegister(bucket, (byte) ((0xff & positionOf1) - registerOffset));
      setNumNonZeroRegisters(numNonZeroRegisters);
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
        context.VERSION == other.context.VERSION, "Version mismatch %d : %d", context.VERSION, other.context.VERSION
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

      otherBuffer.position(HEADER_NUM_BYTES);

      if (isSparse(otherBuffer, other.context)) {
        while (otherBuffer.hasRemaining()) {
          final int position = getUnsignedShort(otherBuffer);
          final byte value = otherBuffer.get();
          numNonZero += mergeAndStoreByteRegister(storageBuffer, position, offsetDiff, value);
        }
      } else { // dense
        int position = HEADER_NUM_BYTES;
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
    if (storageBuffer.remaining() == context.NUM_BYTES_FOR_DENSE_STORAGE && numNonZeroRegisters < DENSE_THRESHOLD) {
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
    scratch.ensureCapacity(HEADER_NUM_BYTES + numNonZeroRegisters * SPARSE_BUCKET_SIZE);

    final byte[] output = scratch.unwrap();
    storageBuffer.get(output, 0, HEADER_NUM_BYTES);
    if (ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(output, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(output, MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
    }

    int x = HEADER_NUM_BYTES;
    if (storageBuffer.hasArray()) {
      final byte[] array = storageBuffer.array();
      final int offset = storageBuffer.arrayOffset();
      for (int i = HEADER_NUM_BYTES; i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
        final byte v = array[offset + i];
        if (v != 0) {
          writeShort(output, x, i);
          output[x + Short.BYTES] = v;
          x += SPARSE_BUCKET_SIZE;
        }
      }
    } else {
      for (int i = HEADER_NUM_BYTES; i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
        final byte v = storageBuffer.get(i);
        if (v != 0) {
          writeShort(output, x, i);
          output[x + Short.BYTES] = v;
          x += SPARSE_BUCKET_SIZE;
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
    if (ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(array, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(array, MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
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

      storageBuffer.position(HEADER_NUM_BYTES);

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
           "version=" + context.VERSION +
           ", registerOffset=" + getRegisterOffset() +
           ", numNonZeroRegisters=" + getNumNonZeroRegisters() +
           ", maxOverflowValue=" + getMaxOverflowValue() +
           ", maxOverflowRegister=" + getMaxOverflowRegister() +
           '}';
  }

  private short decrementBuckets()
  {
    short count = 0;
    for (int i = HEADER_NUM_BYTES; i < context.NUM_BYTES_FOR_DENSE_STORAGE; i++) {
      final byte val = (byte) (storageBuffer.get(i) - 0x11);
      if ((val & 0xf0) != 0) {
        ++count;
      }
      if ((val & 0x0f) != 0) {
        ++count;
      }
      storageBuffer.put(i, val);
    }
    return count;
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
    storageBuffer.get(array, 0, HEADER_NUM_BYTES);
    if (ByteOrder.LITTLE_ENDIAN.equals(storageBuffer.order())) {
      writeShort(array, NUM_NON_ZERO_REGISTERS_BYTE, getNumNonZeroRegisters());
      writeShort(array, MAX_OVERFLOW_REGISTER_BYTE, getMaxOverflowRegister());
    }

    final ByteBuffer tmpBuffer = ByteBuffer.wrap(array);

    tmpBuffer.position(HEADER_NUM_BYTES);
    // put payload
    while (storageBuffer.hasRemaining()) {
      tmpBuffer.put(getUnsignedShort(storageBuffer), storageBuffer.get());
    }
    tmpBuffer.rewind();
    storageBuffer = tmpBuffer;
  }

  private int addNibbleRegister(final int bucket, final byte positionOf1)
  {
    int numNonZeroRegs = getNumNonZeroRegisters();

    final int position = HEADER_NUM_BYTES + (short) (bucket >> 1);
    final boolean isUpperNibble = ((bucket & 0x1) == 0);

    final byte shiftedPositionOf1 = (isUpperNibble) ? (byte) (positionOf1 << BITS_PER_BUCKET) : positionOf1;

    if (storageBuffer.remaining() != context.NUM_BYTES_FOR_DENSE_STORAGE) {
      convertToDenseStorage();
    }

    final byte origVal = storageBuffer.get(position);
    final byte newValueMask = (isUpperNibble) ? (byte) 0xf0 : (byte) 0x0f;
    final byte originalValueMask = (byte) (newValueMask ^ 0xff);

    // if something was at zero, we have to increase the numNonZeroRegisters
    if ((origVal & newValueMask) == 0 && shiftedPositionOf1 != 0) {
      numNonZeroRegs++;
    }

    storageBuffer.put(
        position,
        (byte) (UnsignedBytes.max((byte) (origVal & newValueMask), shiftedPositionOf1) | (origVal & originalValueMask))
    );

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

    final int upperNibble = currVal & 0xf0;
    final int lowerNibble = currVal & 0x0f;

    // subtract the differences so that the nibbles align
    final int otherUpper = (byteToAdd & 0xf0) - (offsetDiff << BITS_PER_BUCKET);
    final int otherLower = (byteToAdd & 0x0f) - offsetDiff;

    final int newUpper = Math.max(upperNibble, otherUpper);
    final int newLower = Math.max(lowerNibble, otherLower);

    storageBuffer.put(position, (byte) ((newUpper | newLower) & 0xff));

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
  public void collect(Object[] values, BytesRef bytes)
  {
    add(Murmur3.hash128(bytes.bytes, 0, bytes.length));
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
}
