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

package io.druid.data.input;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.common.IntTagged;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.YieldingSequenceBase;
import io.druid.query.groupby.UTF8Bytes;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Remove type information & apply compresssion if possible
 */
public class BulkRowSequence extends YieldingSequenceBase<IntTagged<Object[]>>
{
  private static final LZ4Compressor LZ4 = LZ4Factory.fastestInstance().fastCompressor();
  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final Sequence<Object[]> sequence;
  private final int timeIndex;
  private final TimestampRLE timestamps;
  private final int[] category;
  private final Object[] page;
  private final BitSet[] nulls;
  private final int max;

  public BulkRowSequence(Sequence<Object[]> sequence, List<ValueDesc> types, int timeIndex)
  {
    this(sequence, types, timeIndex, DEFAULT_PAGE_SIZE);
  }

  public BulkRowSequence(Sequence<Object[]> sequence, List<ValueDesc> types, int timeIndex, final int max)
  {
    Preconditions.checkArgument(max < 0xffff);    // see TimestampRLE
    this.max = max;
    this.sequence = sequence;
    this.timeIndex = timeIndex;
    this.timestamps = timeIndex < 0 ? null : new TimestampRLE();
    this.category = new int[types.size()];
    this.page = new Object[types.size()];
    this.nulls = new BitSet[types.size()];
    for (int i = 0; i < types.size(); i++) {
      if (i == timeIndex) {
        continue;
      }
      final ValueDesc valueDesc = types.get(i);
      switch (valueDesc.isDimension() ? ValueDesc.typeOfDimension(valueDesc) : valueDesc.type()) {
        case FLOAT:
          category[i] = 0;
          page[i] = new float[max];
          break;
        case LONG:
          category[i] = 1;
          page[i] = new long[max];
          break;
        case DOUBLE:
          category[i] = 2;
          page[i] = new double[max];
          break;
        case BOOLEAN:
          category[i] = 3;
          page[i] = new boolean[max];
          break;
        case STRING:
          category[i] = 4;
          page[i] = new BytesOutputStream(4096);
          break;
        default:
          category[i] = 5;
          page[i] = new Object[max];
      }
      nulls[i] = new BitSet();
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, IntTagged<Object[]>> accumulator)
  {
    BulkYieldingAccumulator<OutType> bulkYielder = new BulkYieldingAccumulator<OutType>(initValue, accumulator);
    return wrapYielder(sequence.toYielder(initValue, bulkYielder), bulkYielder);
  }

  private <OutType> Yielder<OutType> wrapYielder(
      final Yielder<OutType> yielder, final BulkYieldingAccumulator<OutType> accumulator
  )
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        if (yielder.isDone() && accumulator.index > 0) {
          return accumulator.asBulkRow();
        }
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.retValue = initValue;
        return wrapYielder(yielder.next(initValue), accumulator);
      }

      @Override
      public boolean isDone()
      {
        return (yielder == null || yielder.isDone()) && accumulator.index == 0;
      }

      @Override
      public void close() throws IOException
      {
        if (yielder != null) {
          yielder.close();
        }
      }
    };
  }

  private class BulkYieldingAccumulator<OutType> extends YieldingAccumulator<OutType, Object[]>
  {
    private final YieldingAccumulator<OutType, IntTagged<Object[]>> accumulator;

    private int index;
    private OutType retValue;

    public BulkYieldingAccumulator(OutType retValue, YieldingAccumulator<OutType, IntTagged<Object[]>> accumulator)
    {
      this.accumulator = accumulator;
      this.retValue = retValue;
    }

    @Override
    public void reset()
    {
      accumulator.reset();
    }

    @Override
    public boolean yielded()
    {
      return accumulator.yielded();
    }

    @Override
    public void yield()
    {
      accumulator.yield();
    }

    @Override
    public OutType accumulate(OutType prevValue, Object[] values)
    {
      final int ix = index++;

      for (int i = 0; i < category.length; i++) {
        if (i == timeIndex) {
          timestamps.add(((Number) values[i]).longValue());
          continue;
        }
        if (values[i] == null && nulls[i] != null) {
          nulls[i].set(ix, true);
          continue;
        }
        switch (category[i]) {
          case 0: ((float[]) page[i])[ix] = ((Number) values[i]).floatValue(); break;
          case 1: ((long[]) page[i])[ix] = ((Number) values[i]).longValue(); break;
          case 2: ((double[]) page[i])[ix] = ((Number) values[i]).doubleValue(); break;
          case 3: ((boolean[]) page[i])[ix] = (Boolean) values[i]; break;
          case 4:
            final byte[] bytes = values[i] instanceof UTF8Bytes ? ((UTF8Bytes) values[i]).getValue()
                                                                : StringUtils.toUtf8WithNullToEmpty((String) values[i]);
            ((BytesOutputStream) page[i]).writeVarSizeBytes(bytes);
            break;
          default: ((Object[]) page[i])[ix] = values[i]; break;
        }
      }
      return index < max ? prevValue : asBulkRow();
    }

    private OutType asBulkRow()
    {
      final int size = index;
      final Object[] copy = new Object[page.length];

      for (int i = 0; i < category.length; i++) {
        if (i == timeIndex) {
          copy[i] = timestamps.flush();
          continue;
        }
        switch (category[i]) {
          case 0: copy[i] = copy((float[]) page[i], size, nulls[i]); break;
          case 1: copy[i] = copy((long[]) page[i], size, nulls[i]); break;
          case 2: copy[i] = copy((double[]) page[i], size, nulls[i]); break;
          case 3: copy[i] = copy((boolean[]) page[i], size, nulls[i]); break;
          case 4:
            final BytesOutputStream stream = (BytesOutputStream) page[i];
            final byte[] compressed = new byte[Integer.BYTES + LZ4.maxCompressedLength(stream.size())];
            System.arraycopy(Ints.toByteArray(stream.size()), 0, compressed, 0, Integer.BYTES);
            copy[i] = new BytesRef(
                compressed,
                Integer.BYTES + LZ4.compress(stream.toByteArray(), 0, stream.size(), compressed, Integer.BYTES)
            );
            stream.clear();
            break;
          default: copy[i] = Arrays.copyOf((Object[]) page[i], size); break;
        }
      }
      index = 0;
      return retValue = accumulator.accumulate(retValue, IntTagged.of(size, copy));
    }
  }

  private Object copy(final float[] array, final int size, final BitSet nulls)
  {
    if (nulls.isEmpty()) {
      return Arrays.copyOf(array, size);
    }
    final Float[] copy = new Float[size];
    for (int i = 0; i < copy.length; i++) {
      if (!nulls.get(i)) {
        copy[i] = array[i];
      }
    }
    nulls.clear();
    return copy;
  }

  private Object copy(final long[] array, final int size, final BitSet nulls)
  {
    if (nulls.isEmpty()) {
      return Arrays.copyOf(array, size);
    }
    final Long[] copy = new Long[size];
    for (int i = 0; i < copy.length; i++) {
      if (!nulls.get(i)) {
        copy[i] = array[i];
      }
    }
    nulls.clear();
    return copy;
  }

  private Object copy(final double[] array, final int size, final BitSet nulls)
  {
    if (nulls.isEmpty()) {
      return Arrays.copyOf(array, size);
    }
    final Double[] copy = new Double[size];
    for (int i = 0; i < copy.length; i++) {
      if (!nulls.get(i)) {
        copy[i] = array[i];
      }
    }
    nulls.clear();
    return copy;
  }

  private Object copy(final boolean[] array, final int size, final BitSet nulls)
  {
    if (nulls.isEmpty()) {
      return Arrays.copyOf(array, size);
    }
    final Boolean[] copy = new Boolean[size];
    for (int i = 0; i < copy.length; i++) {
      if (!nulls.get(i)) {
        copy[i] = array[i];
      }
    }
    nulls.clear();
    return copy;
  }
}
