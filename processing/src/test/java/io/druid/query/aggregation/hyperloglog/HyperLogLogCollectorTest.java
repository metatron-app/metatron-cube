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

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import io.druid.common.utils.Murmur3;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 */
public class HyperLogLogCollectorTest
{
  private static final HyperLogLogCollector.Context DEFAULT = HyperLogLogCollector.DEFAULT_CTX;

  @Test
  public void testFolding() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        long hashedVal = Murmur3.hash64(random.nextLong());

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();

      folded.fold(oneHalf);
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf);
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }


  /**
   * This is a very long-running test, disabled by default.
   * It is meant to catch issues when combining a large numer of HLL objects.
   *
   * It compares adding all the values to one HLL vs.
   * splitting up values into HLLs of 100 values each, and folding those HLLs into a single main HLL.
   *
   * When reaching very large cardinalities (>> 50,000,000), offsets are mismatched between the main HLL and the ones
   * with 100 values, requiring  a floating max as described in
   * http://druid.io/blog/2014/02/18/hyperloglog-optimizations-for-real-world-systems.html
   */
  @Ignore
  @Test
  public void testHighCardinalityRollingFold() throws Exception
  {
    final HyperLogLogCollector rolling = HyperLogLogCollector.makeLatestCollector();
    final HyperLogLogCollector simple = HyperLogLogCollector.makeLatestCollector();

    MessageDigest md = MessageDigest.getInstance("SHA-1");
    HyperLogLogCollector tmp = HyperLogLogCollector.makeLatestCollector();

    int count;
    for (count = 0; count < 100_000_000; ++count) {
      md.update(Integer.toString(count).getBytes());

      long hashed = Murmur3.hash64(md.digest());

      tmp.add(hashed);
      simple.add(hashed);

      if (count % 100 == 0) {
        rolling.fold(tmp);
        tmp = HyperLogLogCollector.makeLatestCollector();
      }
    }

    int n = count;

    System.out.println("True cardinality " + n);
    System.out.println("Rolling buffer cardinality " + rolling.estimateCardinality());
    System.out.println("Simple  buffer cardinality " + simple.estimateCardinality());
    System.out.printf(
        "Rolling cardinality estimate off by %4.1f%%%n",
        100 * (1 - rolling.estimateCardinality() / n)
    );

    Assert.assertEquals(n, simple.estimateCardinality(), n * 0.05);
    Assert.assertEquals(n, rolling.estimateCardinality(), n * 0.05);
  }

  @Ignore
  @Test
  public void testHighCardinalityRollingFold2() throws Exception
  {
    final HyperLogLogCollector rolling = HyperLogLogCollector.makeLatestCollector();
    int count;
    long start = System.currentTimeMillis();

    for (count = 0; count < 50_000_000; ++count) {
      HyperLogLogCollector theCollector = HyperLogLogCollector.makeLatestCollector();
      theCollector.add(Murmur3.hash64(count));
      rolling.fold(theCollector);
    }
    System.out.printf("testHighCardinalityRollingFold2 took %d ms%n", System.currentTimeMillis() - start);

    int n = count;

    System.out.println("True cardinality " + n);
    System.out.println("Rolling buffer cardinality " + rolling.estimateCardinality());
    System.out.printf(
        "Rolling cardinality estimate off by %4.1f%%%n",
        100 * (1 - rolling.estimateCardinality() / n)
    );

    Assert.assertEquals(n, rolling.estimateCardinality(), n * 0.05);
  }

  @Test
  public void testFoldingByteBuffers() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        long hashedVal = Murmur3.hash64(random.nextLong());

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldingReadOnlyByteBuffers() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        long hashedVal = Murmur3.hash64(random.nextLong());

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.from(
          ByteBuffer.wrap(DEFAULT.makeEmpty())
                    .asReadOnlyBuffer()
      );

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldingReadOnlyByteBuffersWithArbitraryPosition() throws Exception
  {
    final Random random = new Random(0);
    final int[] numValsToCheck = {10, 20, 50, 100, 1000, 2000};
    for (int numThings : numValsToCheck) {
      HyperLogLogCollector allCombined = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oneHalf = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector otherHalf = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        long hashedVal = Murmur3.hash64(random.nextLong());

        allCombined.add(hashedVal);
        if (i % 2 == 0) {
          oneHalf.add(hashedVal);
        } else {
          otherHalf.add(hashedVal);
        }
      }

      HyperLogLogCollector folded = HyperLogLogCollector.from(
          shiftedBuffer(
              ByteBuffer.wrap(DEFAULT.makeEmpty())
                        .asReadOnlyBuffer(),
              17
          )
      );

      folded.fold(oneHalf.toByteBuffer());
      Assert.assertEquals(oneHalf, folded);
      Assert.assertEquals(oneHalf.estimateCardinality(), folded.estimateCardinality(), 0.0d);

      folded.fold(otherHalf.toByteBuffer());
      Assert.assertEquals(allCombined, folded);
      Assert.assertEquals(allCombined.estimateCardinality(), folded.estimateCardinality(), 0.0d);
    }
  }

  @Test
  public void testFoldWithDifferentOffsets1() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x00, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), DEFAULT.BUCKET_MASK);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), DEFAULT.BUCKET_MASK);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }
  }

  @Test
  public void testBufferSwap() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x00, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    ByteBuffer buffer = ByteBuffer.wrap(DEFAULT.makeEmpty());
    HyperLogLogCollector collector = HyperLogLogCollector.from(buffer.duplicate());

    // make sure the original buffer gets modified
    collector.fold(biggerOffset);
    Assert.assertEquals(collector, HyperLogLogCollector.from(buffer.duplicate()));

    // make sure the original buffer gets modified
    collector.fold(smallerOffset);
    Assert.assertEquals(collector, HyperLogLogCollector.from(buffer.duplicate()));
  }

  @Test
  public void testFoldWithArbitraryInitialPositions() throws Exception
  {
    ByteBuffer biggerOffset = shiftedBuffer(makeCollectorBuffer(1, (byte) 0x00, 0x11), 10);
    ByteBuffer smallerOffset = shiftedBuffer(makeCollectorBuffer(0, (byte) 0x20, 0x00), 15);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), DEFAULT.BUCKET_MASK);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), DEFAULT.BUCKET_MASK);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertEquals(outBuffer.get(), 0x10);
    while (outBuffer.hasRemaining()) {
      Assert.assertEquals(outBuffer.get(), 0x11);
    }
  }

  protected ByteBuffer shiftedBuffer(ByteBuffer buf, int offset)
  {
    ByteBuffer shifted = ByteBuffer.allocate(buf.remaining() + offset);
    shifted.position(offset);
    shifted.put(buf);
    shifted.position(offset);
    return shifted;
  }

  @Test
  public void testFoldWithDifferentOffsets2() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, (byte) 0x01, 0x11);
    ByteBuffer smallerOffset = makeCollectorBuffer(0, (byte) 0x20, 0x00);

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(biggerOffset);
    collector.fold(smallerOffset);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());

    collector = HyperLogLogCollector.makeLatestCollector(DEFAULT);
    collector.fold(smallerOffset);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  @Test
  public void testFoldWithUpperNibbleTriggersOffsetChange() throws Exception
  {
    byte[] arr1 = new byte[DEFAULT.NUM_BYTES_FOR_DENSE_STORAGE];
    Arrays.fill(arr1, (byte) 0x11);
    ByteBuffer buffer1 = ByteBuffer.wrap(arr1);
    buffer1.put(HyperLogLogCollector.VERSION_BYTE, DEFAULT.VERSION);
    buffer1.put(HyperLogLogCollector.REGISTER_OFFSET_BYTE, (byte) 0);
    buffer1.putShort(HyperLogLogCollector.NUM_NON_ZERO_REGISTERS_BYTE, (short) (2047));
    buffer1.put(DEFAULT.HEADER_NUM_BYTES, (byte) 0x1);

    byte[] arr2 = new byte[DEFAULT.NUM_BYTES_FOR_DENSE_STORAGE];
    Arrays.fill(arr2, (byte) 0x11);
    ByteBuffer buffer2 = ByteBuffer.wrap(arr2);
    buffer2.put(HyperLogLogCollector.VERSION_BYTE, DEFAULT.VERSION);
    buffer2.put(HyperLogLogCollector.REGISTER_OFFSET_BYTE, (byte) 0);
    buffer2.putShort(HyperLogLogCollector.NUM_NON_ZERO_REGISTERS_BYTE, (short) (2048));

    HyperLogLogCollector collector = HyperLogLogCollector.from(buffer1);
    collector.fold(buffer2);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 1);
    Assert.assertEquals(outBuffer.getShort(), 0);
    outBuffer.get();
    outBuffer.getShort();
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  @Test
  public void testSparseFoldWithDifferentOffsets1() throws Exception
  {
    ByteBuffer biggerOffset = makeCollectorBuffer(1, new byte[]{0x11, 0x10}, 0x11);
    ByteBuffer sparse = HyperLogLogCollector.from(makeCollectorBuffer(0, new byte[]{0x00, 0x02}, 0x00))
                                            .toByteBuffer();

    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector(DEFAULT);
    collector.fold(biggerOffset);
    collector.fold(sparse);

    ByteBuffer outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertFalse(outBuffer.hasRemaining());

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.fold(sparse);
    collector.fold(biggerOffset);

    outBuffer = collector.toByteBuffer();

    Assert.assertEquals(outBuffer.get(), DEFAULT.VERSION);
    Assert.assertEquals(outBuffer.get(), 2);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertEquals(outBuffer.get(), 0);
    Assert.assertEquals(outBuffer.getShort(), 0);
    Assert.assertFalse(outBuffer.hasRemaining());
  }

  private ByteBuffer makeCollectorBuffer(int offset, byte initialBytes, int remainingBytes)
  {
    return makeCollectorBuffer(offset, new byte[]{initialBytes}, remainingBytes);
  }

  private ByteBuffer makeCollectorBuffer(int offset, byte[] initialBytes, int remainingBytes)
  {
    short numNonZero = 0;
    for (byte initialByte : initialBytes) {
      numNonZero += computeNumNonZero(initialByte);
    }

    final short numNonZeroInRemaining = computeNumNonZero((byte) remainingBytes);
    numNonZero += (short)((DEFAULT.NUM_BYTES_FOR_BUCKETS - initialBytes.length) * numNonZeroInRemaining);

    ByteBuffer biggerOffset = ByteBuffer.allocate(DEFAULT.NUM_BYTES_FOR_DENSE_STORAGE);
    biggerOffset.put(DEFAULT.VERSION);
    biggerOffset.put((byte) offset);
    biggerOffset.putShort(numNonZero);
    biggerOffset.put((byte) 0);
    biggerOffset.putShort((short) 0);
    biggerOffset.put(initialBytes);
    while (biggerOffset.hasRemaining()) {
      biggerOffset.put((byte) remainingBytes);
    }
    biggerOffset.clear();
    return biggerOffset.asReadOnlyBuffer();
  }

  private short computeNumNonZero(byte theByte)
  {
    short retVal = 0;
    if ((theByte & 0x0f) > 0) {
      ++retVal;
    }
    if ((theByte & 0xf0) > 0) {
      ++retVal;
    }
    return retVal;
  }

  @Ignore @Test // This test can help when finding potential combinations that are weird, but it's non-deterministic
  public void testFoldingwithDifferentOffsets() throws Exception
  {
    // final Random random = new Random(37); // this seed will cause this test to fail because of slightly larger errors
    final Random random = new Random(0);
    for (int j = 0; j < 10; j++) {
      HyperLogLogCollector smallVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector bigVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector all = HyperLogLogCollector.makeLatestCollector();

      int numThings = 500000;
      for (int i = 0; i < numThings; i++) {
        long hashedVal = Murmur3.hash64(random.nextLong());

        if (i < 1000) {
          smallVals.add(hashedVal);
        } else {
          bigVals.add(hashedVal);
        }
        all.add(hashedVal);
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();
      folded.fold(smallVals);
      folded.fold(bigVals);
      final double expected = all.estimateCardinality();
      Assert.assertEquals(expected, folded.estimateCardinality(), expected * 0.025);
      Assert.assertEquals(numThings, folded.estimateCardinality(), numThings * 0.05);
    }
  }

  @Ignore @Test
  public void testFoldingwithDifferentOffsets2() throws Exception
  {
    final Random random = new Random(0);
    MessageDigest md = MessageDigest.getInstance("SHA-1");

    for (int j = 0; j < 1; j++) {
      HyperLogLogCollector evenVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector oddVals = HyperLogLogCollector.makeLatestCollector();
      HyperLogLogCollector all = HyperLogLogCollector.makeLatestCollector();

      int numThings = 500000;
      for (int i = 0; i < numThings; i++) {
        md.update(Integer.toString(random.nextInt()).getBytes());
        long hashedVal = Murmur3.hash64(random.nextLong());

        if (i % 2 == 0) {
          evenVals.add(hashedVal);
        } else {
          oddVals.add(hashedVal);
        }
        all.add(hashedVal);
      }

      HyperLogLogCollector folded = HyperLogLogCollector.makeLatestCollector();
      folded.fold(evenVals);
      folded.fold(oddVals);
      final double expected = all.estimateCardinality();
      Assert.assertEquals(expected, folded.estimateCardinality(), expected * 0.025);
      Assert.assertEquals(numThings, folded.estimateCardinality(), numThings * 0.05);
    }
  }

  private static final int[] valsToCheck = {10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000};
  private static final double[] expectedVals = {
      10.024493827539368, 19.088683691179465, 48.5714465122633, 96.22559448406093, 988.3287786549159,
      1940.552755020314, 4946.192042635218, 9856.238157443717, 19766.04029402571, 50006.46134633821,
      100147.71106631085, 1003869.1748353346, 2020698.4016976922};


  @Test
  public void testEstimation() throws Exception
  {
    Random random = new Random(0L);

    int valsToCheckIndex = 0;
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(Murmur3.hash64(random.nextLong()));
      if (i == valsToCheck[valsToCheckIndex] - 1) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
  }

  @Test
  public void testEstimationReadOnlyByteBuffers() throws Exception
  {
    Random random = new Random(0L);

    int valsToCheckIndex = 0;
    HyperLogLogCollector collector = HyperLogLogCollector.from(
        ByteBuffer.allocateDirect(DEFAULT.NUM_BYTES_FOR_DENSE_STORAGE)
                  .put(0, DEFAULT.VERSION)
    );
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(Murmur3.hash64(random.nextLong()));
      if (i == valsToCheck[valsToCheckIndex] - 1) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
  }

  @Test
  public void testEstimationLimitDifferentFromCapacity() throws Exception
  {
    Random random = new Random(0L);

    int valsToCheckIndex = 0;
    final ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(10000)
                                                     .put(0, DEFAULT.VERSION)
                                                     .position(0)
                                                     .limit(DEFAULT.NUM_BYTES_FOR_DENSE_STORAGE);
    HyperLogLogCollector collector = HyperLogLogCollector.from(buffer);
    for (int i = 0; i < valsToCheck[valsToCheck.length - 1]; ++i) {
      collector.add(Murmur3.hash64(random.nextLong()));
      if (i == valsToCheck[valsToCheckIndex] - 1) {
        Assert.assertEquals(expectedVals[valsToCheckIndex], collector.estimateCardinality(), 0.0d);
        ++valsToCheckIndex;
      }
    }
  }

  @Test
  @Ignore
  public void test() throws Exception
  {
    final Random random = new Random(0L);
    for (int x : new int[]{1000, 10000, 100000, 1000000, 10000000}) {
      System.out.printf("------------- %,d\n", x);
      for (int b = 11; b <= 24; b++) {
        random.setSeed(0);
        long p = System.currentTimeMillis();
        HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector(b);
        for (int y = 0; y < x; y++) {
          collector.add(Murmur3.hash64(random.nextLong()));
        }
        System.out.printf(
            "%d --> (%d::%d:%d) %.3f%% (%d KB, %d msec)%n",
            b,
            collector.getMaxOverflowValue(),
            collector.getRegisterOffset(),
            collector.getNumNonZeroRegisters(),
            100f * (x - collector.estimateCardinality()) / x,
            collector.toByteArray().length / 1024,
            System.currentTimeMillis() - p
        );
      }
    }
  }

  @Test
  public void testSparseEstimation() throws Exception
  {
    final Random random = new Random(0);
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      collector.add(Murmur3.hash64(random.nextLong()));
    }

    Assert.assertEquals(
        collector.estimateCardinality(), HyperLogLogCollector.estimateByteBuffer(collector.toByteBuffer()), 0.0d
    );
  }

  @Test
  public void testHighBits() throws Exception
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    // fill up all the buckets so we reach a registerOffset of 49
    fillBuckets(collector, (byte) 0, (byte) 49);

    // highest possible bit position is 64
    collector.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
    Assert.assertEquals(8.5089685793441677E17, collector.estimateCardinality(), 1000);

    // this might happen once in a million years if you hash a billion values a second
    fillBuckets(collector, (byte) 0, (byte) 63);
    collector.add(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    Assert.assertEquals(Double.MAX_VALUE, collector.estimateCardinality(), 1000);
  }

  @Test
  public void testCompare1() throws Exception
  {
    HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
    HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
    collector1.add(Murmur3.hash64(0));
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 100; i = i + 2) {
      collector1.add(Murmur3.hash64(i));
      collector1.add(Murmur3.hash64(i + 1));
      Assert.assertEquals(1, comparator.compare(collector1, collector2));
      Assert.assertEquals(1, Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()));
    }
  }

  @Test
  public void testCompare2() throws Exception
  {
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 1000; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(50);
      for (int l = 0; l < j; ++l) {
        collector1.add(Murmur3.hash64(rand.nextLong()));
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 1 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(Murmur3.hash64(rand.nextLong()));
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 100; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(500);
      for (int l = 0; l < j; ++l) {
        collector1.add(Murmur3.hash64(rand.nextLong()));
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 2 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(Murmur3.hash64(rand.nextLong()));
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 10; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(100000);
      for (int l = 0; l < j; ++l) {
        collector1.add(Murmur3.hash64(rand.nextLong()));
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 20000 + rand.nextInt(100000);
      for (int l = 0; l < k; ++l) {
        collector2.add(Murmur3.hash64(rand.nextLong()));
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }
  }

  @Test
  public void testCompareToShouldBehaveConsistentlyWithEstimatedCardinalitiesEvenInToughCases() throws Exception {
    // given
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();

    for (int i = 0; i < 1000; ++i) {
      // given
      HyperLogLogCollector leftCollector = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(9000) + 5000;
      for (int l = 0; l < j; ++l) {
        leftCollector.add(Murmur3.hash64(rand.nextLong()));
      }

      HyperLogLogCollector rightCollector = HyperLogLogCollector.makeLatestCollector();
      int k = rand.nextInt(9000) + 5000;
      for (int l = 0; l < k; ++l) {
        rightCollector.add(Murmur3.hash64(rand.nextLong()));
      }

      // when
      final int orderedByCardinality = Double.compare(leftCollector.estimateCardinality(),
              rightCollector.estimateCardinality());
      final int orderedByComparator = comparator.compare(leftCollector, rightCollector);

      // then, assert hyperloglog comparator behaves consistently with estimated cardinalities
      Assert.assertEquals(
              String.format("orderedByComparator=%d, orderedByCardinality=%d,\n" +
                      "Left={cardinality=%f, hll=%s},\n" +
                      "Right={cardinality=%f, hll=%s},\n", orderedByComparator, orderedByCardinality,
                      leftCollector.estimateCardinality(), leftCollector,
                      rightCollector.estimateCardinality(), rightCollector),
              orderedByCardinality,
              orderedByComparator
      );
    }
  }

  @Test
  public void testMaxOverflow() {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.addOnBucket((short)23, (byte)16);
    Assert.assertEquals(23, collector.getMaxOverflowRegister());
    Assert.assertEquals(16, collector.getMaxOverflowValue());
    Assert.assertEquals(0, collector.getRegisterOffset());
    Assert.assertEquals(0, collector.getNumNonZeroRegisters());

    collector.addOnBucket((short)56, (byte)17);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());

    collector.addOnBucket((short)43, (byte)16);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());
    Assert.assertEquals(0, collector.getRegisterOffset());
    Assert.assertEquals(0, collector.getNumNonZeroRegisters());
  }

  @Test
  public void testMergeMaxOverflow() {
    // no offset
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.addOnBucket((short)23, (byte)16);

    HyperLogLogCollector other = HyperLogLogCollector.makeLatestCollector();
    collector.addOnBucket((short)56, (byte)17);

    collector.fold(other);
    Assert.assertEquals(56, collector.getMaxOverflowRegister());
    Assert.assertEquals(17, collector.getMaxOverflowValue());

    // different offsets
    // fill up all the buckets so we reach a registerOffset of 49
    collector = HyperLogLogCollector.makeLatestCollector();
    fillBuckets(collector, (byte) 0, (byte) 49);
    collector.addOnBucket((short)23, (byte)65);

    other = HyperLogLogCollector.makeLatestCollector();
    fillBuckets(other, (byte) 0, (byte) 43);
    other.addOnBucket((short)47, (byte)67);

    collector.fold(other);
    Assert.assertEquals(47, collector.getMaxOverflowRegister());
    Assert.assertEquals(67, collector.getMaxOverflowValue());
  }


  private static void fillBuckets(HyperLogLogCollector collector, byte startOffset, byte endOffset)
  {
    byte offset = startOffset;
    while (offset <= endOffset) {
      // fill buckets to shift registerOffset
      for (short bucket = 0; bucket < DEFAULT.NUM_BUCKETS; ++bucket) {
        collector.addOnBucket(bucket, offset);
      }
      offset++;
    }
  }

  @Test
  public void testFoldOrder() throws Exception
  {
    final List<String> objects = Lists.newArrayList(
        "AQcH/xYEMXOjRTVSQ1NXVENEM1RTUlVTRDI1aEVnhkOjNUaCI2MkU2VVhVNkNyVTa4NEYkS0kjZYU1RDdEYzUjglNTUzVFM0NkU3ZFUjOVJCdlU0N2QjRDRUV1MyZjNmVDOUM2RVVFRzhnUzVXY1R1RHUnNziURUdmREM0VjVEQmU0aEInZYNzNZNVRFgzVFNolSJHNIQ3QklEZlNSNoNTJXpDk1dFWjJGNYNiQzQkZFNEYzc1NVhSczM2NmJDZlc3JJRCpVNiRlNEI3dmU1ZGI0Q1RCMhNFZEJDZDYyNFOCM3U0VmRlVlNIRVQ4VVw1djNDVURHVSaFU0VEY0U1JFNIVCYlVEJWM2NWU0eURDOjQ6YyNTYkZjNUVjR1ZDdnVkMzVHZFpjMzlmNEFHM0dHJlRYTHSEQjVZVVZkVVIzIjg2SUU0NSM0VFNDNCdGVlQkhBNENCVTZGZEVlxFQyQ0NYWkUmVUJUYzRlNqg4NVVTNThEJkRGNDNUNFSEYmgkR0dDR1JldCNhVEZGRENGc1NDRUNER3WJRTRHQ4JlOYZoJDVVVVMzZSREZ1Q1UjSHNkdUMlU0ODIzZThSNmNDNjQ1o2I0YiRGYyZkNUJYVEMyN2QpQyMkc2VTE4U2VCNHZFRDNTh0IzI2VFNTMlUkNGMlKTRCIyR3QiQzFUNkRTdDM6RDRFI3VyVlcyWCUlQ0YjNjU2Q2dEVFNTRyRlI7VElHVTVVNGk0JHJTQzQkQyVlV0NCVlRkhWYkQ0RVaDNYdFZHWEWFJEYpM0QjNjNVUzNCVzVkgzZGFzQkRZUzN2U1dUFGVWZTUzVUREZDciZEVVYVNjeCU0ZDdEhzIpU2RTOFRUQkWlk1OFRUVTN1MkZSM3ZFc1VDNnUmc2NKNUaUIzd3M0RWxEZTsiNENLVHU0NFUmQ2RWRFdCNUVENFkxZCEnRLQkNEU0RVNmVDQjl9ZmNkM1QVM0MzQkUjJlVHRkNEVWlENDVUIlUvRkM0RVY1UzY6OGVHVCRDIzRUUlUjM2RDWSVkVIU1U1ZiVFNlNDhTN1VWNTVEZ2RzNzVDQlY0ZUNENUM5NUdkRDJGYzRCUzIjRGR4UmJFI4GDRTUiQ0ZUhVY1ZEYoZSRoVDYnREYkQ1SUU0RWUycjp2RZIySVZkUmZDREZVJGQyVEc1JElBZENEU2VEQlVUUnNDQziLRTNidmNjVCtjRFU2Q0SGYzVHVpGTNoVDxFVSMlWTJFQyRJdV1EI3RDloYyNFQ0c1NVY0ZHVEY0dkM2QkQyVDVUVTNFUyamMUdSNrNz0mlFlERzZTSGhFRjVGM3NWU2NINDI2U1RERUhjY4FHNWNTVTV1U0U2I0VXNEZERWNDNUSjI1WmMmQ4U=",
        "AQgH+BUFUEUrZVRjM2IjMzJRESMlUnlTJjEjRhRlNBEyMSUpaGJTMjRCIzMTNCRENRdxNiNEZCQzNERYMiAyIiQmUTI+MhEzV1RWJoMjQjIySDN0QiYDUjUzNjRUVEYyQleDEiUmg0ERRjIjIzJUQjMxNlJGUTNDJFNTRzJiE1M0RjQzUzIiFDUmMjIzJWVCNENTIRJVODUzEkIVMhFEIjM0MkMyIRRCNFNxQyNCQ2UzOFQiJSM0EzU1V1M2EjhUVENDclZzImEiMTJBQlQiJCgyIyKkJSUlNBNDE2M3QSIyMicjMlJEUhJDJFQjJ0VSQ0QyYSFhZSNlQ4REUzVFIlOFRHIkYUJEM8RVMkMiMEczQwMlE1EkAlNiQlhCNkISRVI0ITUjRDU1JVNlK1QyGGRHQVM0NUVHQ1MkMyQoIzMzFCFUI0IhU1OIhCIlZUQVIUMyYzMlMUZ0RCKEIigUIlQ0QkQTM0MkM0QyJkUSM2I2tHJDUTQ0RBQ0YyNlUxUzIiIiMUiSMzUlJDNDQjM0ITQyNIM1MyNWM0MDOTZYVDRWIiZhMzc0NCJ0Q0NDZEMUElMyRyMmUhNiMkIZNjMkEyRTIzYkMzNUODUTNDJVM0ZTQjFCJCNWSTUlEiNCM1U2FCZUJzMVMyLjNkMhITVDEjIYMzNiVmIlO1VTMjMiVDQ2NTJFYyE0Q2IjRDN2IjRTRUVTFUVEYVKBVSMVJSFE0zOXNSJIqVElMVM4MiZEFSMhRlJEJUZnMycmQmQyJDl1JzVjMXQ0MzMjE1VUI1JDJUQyYRQ2JVZzQUJDM2IyInEkY1QiZTJEMRMiMxRVNEUjJUNkJHNSQiNCVCIyIjJUQlEhNUdFUhQzgkcSZaJUVUM0YiJEM2SjczUUIUIlQiM0RiQkIzZhRBJSRzQ0ZUI00UUSRSQlQmMkNINzODQhJFRTZ0FRQ3QTRhIzFTJFRBMmMzQzQhZENUMiIlV2VEMiNFRWQ1F1IyFXRSUyRTMqZ3I0YyhUNEJRMjISZRc2NDOEIjIxVGVWIXYyMiNCJBFDQSMhIzMjVFIDElgyJCUyVFgkRSQzIjJFQlNWRTQWMmQzFFOiMzVTZGMxNFZUNmIjRjETNUNURERTQjYVIkEzNEEyNDNTVUJSVzVkMjEyUlMjQ0RGgyFFNUQhRGMmRUQ2ZSOFETUYNlZCUhRiU2QhVUUiIlJDRjMhRVJDZxNSRTNBRCEoI0FGNUVRE0VFOGdCRDM2QkJCFSQhMxITRoE0VFIzVWUiUTNkRhNDMiMmIzRDQSNTFDoldaJDcnNjkSMJg3IkIiRENSQmciUhY2NFQ4RSNoJENkWDMmVCJGMxQjJGJScyNTJDVDNEEiZSMzQyIyVGRTNEIUw=",
        "AQgH+hQAFyMzlFVXNCNlRxRUYlRUUUZCMnRFJiR0WTgyZiRJZzRFQkVTVVVWc2ZFMlY1QkIxYUQTI0JDY1YkNEVENGUuQTRiNkQ0VUEzNkKUKLSIVkUhNiZURnRFMzcjVEBTdjVVVCIzJDM0hjc0RDVlVjRqMjJVZTNSM0QmQyMTRlNzVCNERFQyMxNBZHMiUSdYIUUjNlVjNzRyYWFHRHI3hKMnYnhFNCZOdlNUZBM0Q0clNTVBiEQRMUQzNSNVQ0IkEmZYNzIyNkRSUik2VBOVRCRDg0IilEMlcjRJMkJDSjRCJURTVDJBMmRTVBM1YyRRMSQoRDV2YzRDVCUkQWFFNDYnQ0IkUzRjRkQ1dGI0VUYzRERCQ1I2dFNhREOUUjJDc0NTN0JFNUZJRGFpU1Q0QyJlNiMzNCZSKFQzYnNUWTMiRGMiRWdSQzMiQnQ0QSgjVUMiE0hRM1NVUiZVIlRkRVMzI2VkRjQWQ1YyRiZWNHQXQ0UllUMSVTJDQzMkWCQiRFglMzIzKEYzJSJFMyREVIQlVFFlYzMDQyVWUZNCQlM0NUJFIkWiNnREdEJDImNWJDOIcmKyQzc5VDVRQ3PVNjQzIkJTQ3FzMjMyRFVFVTUlNUZEMzEjI0Q0M0Y2U1JTREQjIhZScUJjQkYhFRJFQyI0pTVmFTVlMkJXNDI1U3dFZkR2U0NCVRQyRih0UkIhckRUY0ZHSG00EiJUdVIxVjVGNnUVZCxEQkNTQjQ0IkZDciIkODYxM1MzRZRHQxVEZHZWJFIzRRZjVDNBMzI1Q1FEhUMiI0NkJWJWJDJzYlQiRSQjRoRiRhJTIjNSRVJEM1MiYmUiNBkjFkczRWU1SURIJUVDRFQ0QyZCUlRENEImE2FDQxRjlEdTI3RSNEU3RGJyWDNVMTVJNDM1QkJFQmNWRXUlcxNEQzNTGCtDUlNDMzMzY2VlcUQlaUIyZVMzA3NFM1NDc0JjZDUkQiFDY3QUczQzUkVDQjMiUWQ0NEQyNRVTMRJFM2RUMZNSQkQ0MkIiUgGCUkRig1UiElQkdDJFJDciVGIxMjQzI1UlNlRTM1JkRDc+RSM0VFUzMjWCU0RDMxJyJVJGI1VTEUQyM1R0I1c0NFNTM3MhIlUkNFIlZGNURkVURyNIVCMyYzQmQjITRkVHQ2NINGQ0Y0UW0icyUzMydEVBJVJIJENkUjRVIjQSNVYnEzVYMzUmYzGVNFRiQk0iVTVCM0RjJSMyRWRSQkURJBR0M0NzhnRlM3IzQxMTRDJjM1UVUkJCNUQTVGQlEzN0VDMyM0MmO2QoQzNSVURhFEAkU2IldINHRUU00zNFJVQxUkZEcVMyJSJkQjKFNCNUOzIYJEHEUyKCQjJESSY=",
        "AQcH/x0BbjQ2JTpUlkdFRERHVDRkWDU0RGR1ejRURHZ6IzUqdJN1M1VFQiNHI1NTI0J1VHOGZYVFVTRIRJVkVmUolWVShERjSDRVMlRlJDU2VFh3UmR1Mjg3K0M2SUY0Q0ZUspNiJEdZMmc3YkxGSERGOGdjgzNRVGM1Q1UnN0RHU1Y0WWUzRWVEJSRSeGQ0RlNFJVVJU3YoQkdEQ2M2MiVFUyJWRUVWNmRkM0NkVER2WXNkR0QlNGNEVlYzZSS4RDMyVEQ1ckRTM0ZoMlQ2tURGQ0OFQ0ZiY1ZFNEajdXEjVSI6ZWSjNHVRRTRVMldzUjm0NGU0dlhESFRDM0IzVCYkdjdlJJRFVDaHEzUkRmNWOEVXZTM0U0VkREdSUjRHVVViVCVFVUN0RDNDkl01VHMoNVQzYlZFZmNVVUNDQ1VjUiQ2NTV0UzZVModSNEY4Zpc2JjhjiFJVUGM0SHI0UzRTU1R2R0d3VENUZSQzRUZlY4d0aGNkhTQzWVZFZTZkJ2NEZaVDU1alJWpFJpRGRnIlZUU1ZUR2M1NzOkVEMzVjZERiVlRYSEkmU4RLM0RTQ2Q2RjM3RTNhdVVEQzRXJUZTRmM1OEZTYyJkRGRjZDVTlDhSMzdXQiU1RFUiIoRpVGlXIjY1UVVjc0RDJDNSM0NVJTNkRUU1U0lDdEVXY2NGVVNJVmJJRTREVVNiMyVIQ3U6O0U0M0MzZFVVIzJmNERWJaJjikIlRXk1hFQ2NEU0RUN4UzdENEsVgzZFVidXUnU2VRZFRUQmZmRERCQ0ZER2Q3YnZFNlVpJUkzZVREKFWEUzVVMzYzQzQhfTYzQ0IlI5UoV0RGJCVXSDkyZCRSU3ITUkNoYzJUMkYzhlVVRTNyaDNmQzRDVVRjNkVUhEJyRBR2JlOEREVUU0RjY4Nkc3ZERGUyVDNFZGNFOTY3U1OKNlkjQy1TVlRTQ0M1REU2QhgzUzUzOWlWQ1Z3RTQzIzc7RXVkI0M4NCNYRVRGNEZbhFEyVJI0R1OUZEQ3VUVEQlU1NkNYJEYzdSQ0ZSNGeEWIVVU3KEVFY1RZQ0JSNEJFNFMyM0UzN0hHNTQjMlRGNkiEMyVjRFNVRXNkZGM2M4hENCMnU1VWQjNFRkO2VmO1RndEVzWTQiiHQ0NzM2clM4NjQxpjQjZEVTNEpEdlREJzc3OjZnRlNFNWJVNFeDokNCRmQ5NURJVUZSJyRDRXikVURVITZDNGW0ITNEOUQ0RUklZDQjYjVENURDRCRmRDU1hCY2VTR0RGIzJSZzlSczdTFJJkRlZyU1M1JTdVhDYhVFczQ0hTRIc0RCNDdUJEQxNlZEQ2ZEUiJJRFU3YzVGRER0R2ZlNFOTU1MyRGI0RzMkQ2Q="
    );

    List<HyperLogLogCollector> collectors = Lists.transform(
        objects, s -> HyperLogLogCollector.from(ByteBuffer.wrap(Base64.decodeBase64(s)))
    );

    Collection<List<HyperLogLogCollector>> permutations = Collections2.permutations(collectors);

    for(List<HyperLogLogCollector> permutation : permutations) {
      HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

      for (HyperLogLogCollector foldee : permutation) {
        collector.fold(foldee);
      }
      Assert.assertEquals(29, collector.getMaxOverflowValue());
      Assert.assertEquals(366, collector.getMaxOverflowRegister());
      Assert.assertEquals(1.0429189446653817E7, collector.estimateCardinality(), 1);
    }
  }

  // Provides a nice printout of error rates as a function of cardinality
  @Test
  public void showErrorRate() throws Exception
  {
    Random random = new Random(0);

    double error = 0.0d;
    int count = 0;

    final int[] valsToCheck = {
        10, 20, 50, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1000000, 2000000, 10000000
    };

    for (int numThings : valsToCheck) {
      long startTime = System.currentTimeMillis();
      HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

      for (int i = 0; i < numThings; ++i) {
        if (i != 0 && i % 100000000 == 0) {
          ++count;
          error = computeError(error, count, i, startTime, collector);
        }
        collector.add(Murmur3.hash64(random.nextLong()));
      }

      ++count;
      error = computeError(error, count, numThings, startTime, collector);
    }
  }

  @Test
  public void test3947()
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add(Murmur3.hash64("4".getBytes()));
    collector.add(Murmur3.hash64("43".getBytes()));
    Assert.assertEquals(2, collector.estimateCardinality(), 0.1);

    collector = HyperLogLogCollector.makeLatestCollector();
    collector.add(Murmur3.hash128("4".getBytes()));
    collector.add(Murmur3.hash128("43".getBytes()));
    Assert.assertEquals(1, collector.estimateCardinality(), 0.1);
  }

  private double computeError(double error, int count, int numThings, long startTime, HyperLogLogCollector collector)
  {
    final double estimatedValue = collector.estimateCardinality();
    final double errorThisTime = Math.abs((double) numThings - estimatedValue) / numThings;

    error += errorThisTime;

    System.out.printf(
        "%,d ==? %,f in %,d millis. actual error[%,f%%], avg. error [%,f%%]%n",
        numThings,
        estimatedValue,
        System.currentTimeMillis() - startTime,
        100 * errorThisTime,
        (error / count) * 100
    );
    return error;
  }
}
