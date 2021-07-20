/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.query.aggregation.bloomfilter;

import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.Murmur3;
import io.druid.data.input.BytesInputStream;
import io.druid.data.input.BytesOutputStream;
import io.druid.query.aggregation.HashCollector;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This is a direct modification of the Apache Hive 'BloomKFilter', found at:
 * https://github.com/apache/hive/blob/rel/storage-release-2.7.0/storage-api/src/java/org/apache/hive/common/util/BloomKFilter.java
 * modified to store variables which are re-used instead of re-allocated per call as {@link ThreadLocal} so multiple
 * threads can share the same filter object. Note that this is snapshot at hive-storag-api version 2.7.0, latest
 * versions break compatibility with how int/float are stored in a bloom filter in this commit:
 * https://github.com/apache/hive/commit/87ce36b458350db141c4cb4b6336a9a01796370f#diff-e65fc506757ee058dc951d15a9a526c3L238
 * and this linked issue https://issues.apache.org/jira/browse/HIVE-20101.
 *
 * Addtionally, a handful of methods have been added to in situ work with BloomKFilters that have been serialized to a
 * ByteBuffer, e.g. all add and merge methods. Test methods were not added because we don't need them.. but would
 * probably be chill to do so it is symmetrical.
 *
 * Todo: remove this and begin using hive-storage-api version again once
 *  https://issues.apache.org/jira/browse/HIVE-20893 is released and if/when static ByteBuffer methods have been merged
 *  (or alternatively, move them to some sort of utils class)
 *
 * begin copy-pasta:
 *
 * BloomKFilter is variation of {@link org.apache.hive.common.util.BloomFilter}. Unlike BloomFilter, BloomKFilter will spread
 * 'k' hash bits within same cache line for better L1 cache performance. The way it works is,
 * First hash code is computed from key which is used to locate the block offset (n-longs in bitset constitute a block)
 * Subsequent 'k' hash codes are used to spread hash bits within the block. By default block size is chosen as 8,
 * which is to match cache line size (8 longs = 64 bytes = cache line size).
 * Refer {@link BloomKFilter#addBytes(byte[])} for more info.
 *
 * This implementation has much lesser L1 data cache misses than {@link org.apache.hive.common.util.BloomFilter}.
 */
public class BloomKFilter implements HashCollector.ScanSupport
{
  public static final float DEFAULT_FPP = 0.05f;
  // Given a byte array consisting of a serialized BloomKFilter, gives the offset (from 0)
  // for the start of the serialized long values that make up the bitset.
  // NumHashFunctions (1 byte) + bitset array length (4 bytes)
  public static final int START_OF_SERIALIZED_LONGS = 5;
  private static final int DEFAULT_BLOCK_SIZE = 8;
  private static final int DEFAULT_BLOCK_SIZE_BITS = (int) (Math.log(DEFAULT_BLOCK_SIZE) / Math.log(2));
  private static final int DEFAULT_BLOCK_OFFSET_MASK = DEFAULT_BLOCK_SIZE - 1;
  private static final int DEFAULT_BIT_OFFSET_MASK = Long.SIZE - 1;
  private static final ThreadLocal<byte[]> BYTE_ARRAY_4 = ThreadLocal.withInitial(() -> new byte[4]);
  private final BitSet bitSet;
  private final int m;
  private final int k;
  // spread k-1 bits to adjacent longs, default is 8
  // spreading hash bits within blockSize * longs will make bloom filter L1 cache friendly
  // default block size is set to 8 as most cache line sizes are 64 bytes and also AVX512 friendly
  private final int totalBlockCount;

  public BloomKFilter(long maxNumEntries)
  {
    checkArgument(maxNumEntries > 0, "expectedEntries should be > 0");
    long numBits = optimalNumOfBits(maxNumEntries, DEFAULT_FPP);
    this.k = optimalNumOfHashFunctions(maxNumEntries, numBits);
    int nLongs = (int) Math.ceil((double) numBits / (double) Long.SIZE);
    // additional bits to pad long array to block size
    int padLongs = DEFAULT_BLOCK_SIZE - nLongs % DEFAULT_BLOCK_SIZE;
    this.m = (nLongs + padLongs) * Long.SIZE;
    this.bitSet = new BitSet(m);
    checkArgument((bitSet.data.length % DEFAULT_BLOCK_SIZE) == 0, "bitSet has to be block aligned");
    this.totalBlockCount = bitSet.data.length / DEFAULT_BLOCK_SIZE;
  }

  /**
   * A constructor to support rebuilding the BloomFilter from a serialized representation.
   *
   * @param bits
   * @param numFuncs
   */
  public BloomKFilter(long[] bits, int numFuncs)
  {
    super();
    bitSet = new BitSet(bits);
    this.m = bits.length * Long.SIZE;
    this.k = numFuncs;
    checkArgument((bitSet.data.length % DEFAULT_BLOCK_SIZE) == 0, "bitSet has to be block aligned");
    this.totalBlockCount = bitSet.data.length / DEFAULT_BLOCK_SIZE;
  }

  static void checkArgument(boolean expression, String message)
  {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  static int optimalNumOfHashFunctions(long n, long m)
  {
    return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
  }

  static long optimalNumOfBits(long n, double p)
  {
    return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
  }

  /**
   * Merges BloomKFilter bf2 into bf1.
   * Assumes 2 BloomKFilters with the same size/hash functions are serialized to byte arrays
   *
   * @param bf1Bytes
   * @param bf1Start
   * @param bf1Length
   * @param bf2Bytes
   * @param bf2Start
   * @param bf2Length
   */
  public static void mergeBloomFilterBytes(
      byte[] bf1Bytes,
      int bf1Start,
      int bf1Length,
      byte[] bf2Bytes,
      int bf2Start,
      int bf2Length
  )
  {
    if (bf1Length != bf2Length) {
      throw new IllegalArgumentException("bf1Length " + bf1Length + " does not match bf2Length " + bf2Length);
    }

    // Validation on the bitset size/3 hash functions.
    for (int idx = 0; idx < START_OF_SERIALIZED_LONGS; ++idx) {
      if (bf1Bytes[bf1Start + idx] != bf2Bytes[bf2Start + idx]) {
        throw new IllegalArgumentException("bf1 NumHashFunctions/NumBits does not match bf2");
      }
    }

    // Just bitwise-OR the bits together - size/# functions should be the same,
    // rest of the data is serialized long values for the bitset which are supposed to be bitwise-ORed.
    for (int idx = START_OF_SERIALIZED_LONGS; idx < bf1Length; ++idx) {
      bf1Bytes[bf1Start + idx] |= bf2Bytes[bf2Start + idx];
    }
  }

  /**
   * Merges BloomKFilter bf2Buffer into bf1Buffer in place. Does not mutate buffer positions.
   * Assumes 2 BloomKFilters with the same size/hash functions are serialized to ByteBuffers
   *
   * @param bf1Buffer
   * @param bf1Start
   * @param bf2Buffer
   * @param bf2Start
   */
  public static void mergeBloomFilterByteBuffers(
      ByteBuffer bf1Buffer,
      int bf1Start,
      ByteBuffer bf2Buffer,
      int bf2Start
  )
  {
    ByteBuffer view1 = bf1Buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    ByteBuffer view2 = bf2Buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    final int bf1Length = START_OF_SERIALIZED_LONGS + (view1.getInt(1 + bf1Start) * Long.BYTES);
    final int bf2Length = START_OF_SERIALIZED_LONGS + (view2.getInt(1 + bf2Start) * Long.BYTES);

    if (bf1Length != bf2Length) {
      throw new IllegalArgumentException("bf1Length " + bf1Length + " does not match bf2Length " + bf2Length);
    }

    // Validation on the bitset size/3 hash functions.
    for (int idx = 0; idx < START_OF_SERIALIZED_LONGS; ++idx) {
      if (view1.get(bf1Start + idx) != view2.get(bf2Start + idx)) {
        throw new IllegalArgumentException("bf1 NumHashFunctions/NumBits does not match bf2");
      }
    }

    // Just bitwise-OR the bits together - size/# functions should be the same,
    // rest of the data is serialized long values for the bitset which are supposed to be bitwise-ORed.
    for (int idx = START_OF_SERIALIZED_LONGS; idx < bf1Length; ++idx) {
      final int pos1 = bf1Start + idx;
      final int pos2 = bf2Start + idx;
      view1.put(pos1, (byte) (view1.get(pos1) | view2.get(pos2)));
    }
  }

  @JsonValue
  public byte[] serialize()
  {
    BytesOutputStream output = new BytesOutputStream();
    output.write(k);
    output.writeInt(bitSet.data.length);
    for (long x : bitSet.data) {
      output.writeLong(x);
    }
    return output.toByteArray();
  }

  public int length()
  {
    return Byte.BYTES + Integer.BYTES + bitSet.bitSize();
  }

  public static void init(ByteBuffer in, int position, int maxNumEntries)
  {
    ByteBuffer duplicated = in.duplicate().order(ByteOrder.BIG_ENDIAN);
    duplicated.position(position);
    duplicated.put((byte) 0);
    duplicated.putInt(-1);
    int numLongs = computeSizeBytes(maxNumEntries) >> 3;
    for (int x = 0; x < numLongs; x++) {
      duplicated.putLong(0L);
    }
  }

  public static BloomKFilter deserialize(byte[] bytes)
  {
    BytesInputStream input = new BytesInputStream(bytes);
    int numHashFunc = input.readByte();
    int bitsetArrayLen = input.readInt();
    if (bitsetArrayLen < 0) {
      return null;
    }
    final long[] data = new long[bitsetArrayLen];
    for (int i = 0; i < bitsetArrayLen; i++) {
      data[i] = input.readLong();
    }
    return new BloomKFilter(data, numHashFunc);
  }

  public static BloomKFilter deserialize(ByteBuffer in, int position)
  {
    ByteBuffer duplicated = in.duplicate().order(ByteOrder.BIG_ENDIAN);
    duplicated.position(position);
    int numHashFunc = duplicated.get();
    int bitsetArrayLen = duplicated.getInt();
    if (numHashFunc == 0 || bitsetArrayLen < 0) {
      return null;
    }
    final long[] data = new long[bitsetArrayLen];
    for (int i = 0; i < bitsetArrayLen; i++) {
      data[i] = duplicated.getLong();
    }
    return new BloomKFilter(data, numHashFunc);
  }

  /**
   * ByteBuffer based copy of logic of {@link BloomKFilter#getNumSetBits()}
   * @param buffer
   * @param position
   * @return
   */
  public static int getNumSetBits(ByteBuffer buffer, int position)
  {
    ByteBuffer view = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    view.position(position);
    int numLongs = view.getInt(1 + position);
    int setBits = 0;
    for (int i = 0, pos = START_OF_SERIALIZED_LONGS + position; i < numLongs; i++, pos += Long.BYTES) {
      setBits += Long.bitCount(view.getLong(pos));
    }
    return setBits;
  }

  /**
   * Calculate size in bytes of a BloomKFilter for a given number of entries
   */
  public static int computeSizeBytes(long maxNumEntries)
  {
    // copied from constructor
    checkArgument(maxNumEntries > 0, "expectedEntries should be > 0");
    long numBits = optimalNumOfBits(maxNumEntries, DEFAULT_FPP);

    int nLongs = (int) Math.ceil((double) numBits / (double) Long.SIZE);
    int padLongs = DEFAULT_BLOCK_SIZE - nLongs % DEFAULT_BLOCK_SIZE;
    return START_OF_SERIALIZED_LONGS + ((nLongs + padLongs) * Long.BYTES);
  }

  private void addHash(long hash64)
  {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);

    int firstHash = hash1 + hash2;
    // hashcode should be positive, flip all the bits if it's negative
    if (firstHash < 0) {
      firstHash = ~firstHash;
    }

    // first hash is used to locate start of the block (blockBaseOffset)
    // subsequent K hashes are used to generate K bits within a block of words
    final int blockIdx = firstHash % totalBlockCount;
    final int blockBaseOffset = blockIdx << DEFAULT_BLOCK_SIZE_BITS;
    for (int i = 1; i <= k; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      // LSB 3 bits is used to locate offset within the block
      final int absOffset = blockBaseOffset + (combinedHash & DEFAULT_BLOCK_OFFSET_MASK);
      // Next 6 bits are used to locate offset within a long/word
      final int bitPos = (combinedHash >>> DEFAULT_BLOCK_SIZE_BITS) & DEFAULT_BIT_OFFSET_MASK;
      bitSet.data[absOffset] |= (1L << bitPos);
    }
  }

  /**
   * ByteBuffer based copy of {@link BloomKFilter#addHash(long)} that adds a value to the ByteBuffer in place.
   */
  private static void addHash(final ByteBuffer buffer, final int position, final long hash64)
  {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);

    int firstHash = hash1 + hash2;
    // hashcode should be positive, flip all the bits if it's negative
    if (firstHash < 0) {
      firstHash = ~firstHash;
    }

    final ByteBuffer view = buffer.duplicate().order(ByteOrder.BIG_ENDIAN);
    final int numHashFuncs = view.get(position);
    final int totalBlockCount = view.getInt(position + 1) / DEFAULT_BLOCK_SIZE;
    // first hash is used to locate start of the block (blockBaseOffset)
    // subsequent K hashes are used to generate K bits within a block of words
    final int blockIdx = firstHash % totalBlockCount;
    final int blockBaseOffset = blockIdx << DEFAULT_BLOCK_SIZE_BITS;
    for (int i = 1; i <= numHashFuncs; i++) {
      int combinedHash = hash1 + (i + 1) * hash2;
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      // LSB 3 bits is used to locate offset within the block
      final int absOffset = blockBaseOffset + (combinedHash & DEFAULT_BLOCK_OFFSET_MASK);
      // Next 6 bits are used to locate offset within a long/word
      final int bitPos = (combinedHash >>> DEFAULT_BLOCK_SIZE_BITS) & DEFAULT_BIT_OFFSET_MASK;

      final int bufPos = position + START_OF_SERIALIZED_LONGS + (absOffset * Long.BYTES);
      view.putLong(bufPos, view.getLong(bufPos) | (1L << bitPos));
    }
  }

  @Override
  public void collect(Object[] values, BytesRef key)
  {
    addHash(Murmur3.hash64(key.bytes, 0, key.length));
  }

  @Override
  public void collect(DimensionSelector.Scannable scannable)
  {
    addHash(scannable.apply((ix, buffer, offset, length) -> Murmur3.hash64wrap(buffer, offset, length)).longValue());
  }

  public static void collect(ByteBuffer collector, int position, BytesRef key)
  {
    addHash(collector, position, Murmur3.hash64(key.bytes, 0, key.length));
  }

  public static void collect(ByteBuffer collector, int position, DimensionSelector.Scannable scannable)
  {
    addHash(
        collector,
        position,
        scannable.apply((ix, buffer, offset, length) -> Murmur3.hash64wrap(buffer, offset, length)).longValue()
    );
  }

  public boolean test(BytesRef key)
  {
    return testHash(Murmur3.hash64(key.bytes, 0, key.length));
  }

  public boolean test(DimensionSelector.Scannable scannable)
  {
    return testHash(
        scannable.apply((ix, buffer, offset, length) -> Murmur3.hash64wrap(buffer, offset, length)).longValue()
    );
  }

  public boolean testHash(long hash64)
  {
    final int hash1 = (int) hash64;
    final int hash2 = (int) (hash64 >>> 32);

    int firstHash = hash1 + hash2;
    // hashcode should be positive, flip all the bits if it's negative
    if (firstHash < 0) {
      firstHash = ~firstHash;
    }

    // first hash is used to locate start of the block (blockBaseOffset)
    // subsequent K hashes are used to generate K bits within a block of words
    // To avoid branches during probe, a separate masks array is used for each longs/words within a block.
    // data array and masks array are then traversed together and checked for corresponding set bits.
    final int blockIdx = firstHash % totalBlockCount;
    final int blockBaseOffset = blockIdx << DEFAULT_BLOCK_SIZE_BITS;


    // iterate and update masks array
    for (int i = 1; i <= k; i++) {
      int combinedHash = hash1 + ((i + 1) * hash2);
      // hashcode should be positive, flip all the bits if it's negative
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      // LSB 3 bits is used to locate offset within the block
      final int wordOffset = combinedHash & DEFAULT_BLOCK_OFFSET_MASK;
      final int absOffset = blockBaseOffset + wordOffset;

      // Next 6 bits are used to locate offset within a long/word
      final int bitPos = (combinedHash >>> DEFAULT_BLOCK_SIZE_BITS) & DEFAULT_BIT_OFFSET_MASK;
      final long bloomWord = bitSet.data[absOffset];
      if (0 == (bloomWord & (1L << bitPos))) {
        return false;
      }
    }
    return true;
  }

  public long sizeInBytes()
  {
    return getBitSize() / 8;
  }

  public int getBitSize()
  {
    return bitSet.getData().length * Long.SIZE;
  }

  public int getNumSetBits()
  {
    int setCount = 0;
    for (long datum : bitSet.getData()) {
      setCount += Long.bitCount(datum);
    }
    return setCount;
  }

  public int getNumHashFunctions()
  {
    return k;
  }

  public int getNumBits()
  {
    return m;
  }

  public long[] getBitSet()
  {
    return bitSet.getData();
  }

  @Override
  public String toString()
  {
    return "m: " + m + " k: " + k;
  }

  /**
   * Merge the specified bloom filter with current bloom filter.
   *
   * @param that - bloom filter to merge
   */
  public BloomKFilter merge(BloomKFilter that)
  {
    if (this != that && this.m == that.m && this.k == that.k) {
      this.bitSet.putAll(that.bitSet);
    } else {
      throw new IllegalArgumentException("BloomKFilters are not compatible for merging." +
                                         " this - " + this + " that - " + that);
    }
    return this;
  }

  public void reset()
  {
    this.bitSet.clear();
  }

  /**
   * Bare metal bit set implementation. For performance reasons, this implementation does not check
   * for index bounds nor expand the bit set size if the specified index is greater than the size.
   */
  public static class BitSet
  {
    private final long[] data;

    public BitSet(long bits)
    {
      this(new long[(int) Math.ceil((double) bits / (double) Long.SIZE)]);
    }

    /**
     * Deserialize long array as bit set.
     *
     * @param data - bit array
     */
    public BitSet(long[] data)
    {
      assert data.length > 0 : "data length is zero!";
      this.data = data;
    }

    /**
     * Sets the bit at specified index.
     *
     * @param index - position
     */
    public void set(int index)
    {
      data[index >>> 6] |= (1L << index);
    }

    /**
     * Returns true if the bit is set in the specified index.
     *
     * @param index - position
     *
     * @return - value at the bit position
     */
    public boolean get(int index)
    {
      return (data[index >>> 6] & (1L << index)) != 0;
    }

    /**
     * Number of bits
     */
    public int bitSize()
    {
      return data.length * Long.SIZE;
    }

    public long[] getData()
    {
      return data;
    }

    /**
     * Combines the two BitArrays using bitwise OR.
     */
    public void putAll(BloomKFilter.BitSet array)
    {
      assert data.length == array.data.length :
          "BitArrays must be of equal length (" + data.length + "!= " + array.data.length + ")";
      for (int i = 0; i < data.length; i++) {
        data[i] |= array.data[i];
      }
    }

    /**
     * Clear the bit set.
     */
    public void clear()
    {
      Arrays.fill(data, 0);
    }
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof BloomKFilter)) {
      return false;
    }
    BloomKFilter other = (BloomKFilter) obj;
    if (k != other.k) {
      return false;
    }
    if (bitSet.data.length != other.bitSet.data.length) {
      return false;
    }
    for (int i = 0; i < bitSet.data.length; i++) {
      if (bitSet.data[i] != other.bitSet.data[i]) {
        return false;
      }
    }
    return true;
  }
}
