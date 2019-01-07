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

package io.druid.data.input;

import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.guava.YieldingSequenceBase;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.groupby.UTF8Bytes;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.python.google.common.primitives.Ints;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Remove type information & apply compresssion if possible
 */
public class BulkRowSequence extends YieldingSequenceBase<Row>
{
  private static final LZ4Compressor LZ4 = LZ4Factory.fastestInstance().fastCompressor();
  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final Sequence<Row> sequence;
  private final TimestampRLE timestamps;
  private final int[] category;
  private final Object[] page;
  private final int max;

  public BulkRowSequence(final Sequence<Row> sequence, final List<ValueDesc> types)
  {
    this(sequence, types, DEFAULT_PAGE_SIZE);
  }

  public BulkRowSequence(final Sequence<Row> sequence, final List<ValueDesc> types, final int max)
  {
    this.sequence = sequence;
    this.timestamps = new TimestampRLE();
    this.category = new int[types.size()];
    this.page = new Object[types.size()];
    for (int i = 1; i < types.size(); i++) {
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
        case STRING:
          category[i] = 3;
          page[i] = new BytesOutputStream(4096);
          break;
        default:
          category[i] = 4;
          page[i] = new Object[max];
      }
    }
    this.max = Math.min(0xffff, max);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, Row> accumulator)
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

  private class BulkYieldingAccumulator<OutType> extends YieldingAccumulator<OutType, Row>
  {
    private final YieldingAccumulator<OutType, Row> accumulator;

    private int index;
    private OutType retValue;

    public BulkYieldingAccumulator(OutType retValue, YieldingAccumulator<OutType, Row> accumulator)
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
    public OutType accumulate(OutType prevValue, Row current)
    {
      final int ix = index++;
      final Object[] values = ((CompactRow) current).getValues();

      timestamps.add(((Number) values[0]).longValue());
      for (int i = 1; i < category.length; i++) {
        switch (category[i]) {
          case 0: ((float[]) page[i])[ix] = ((Number) values[i]).floatValue(); break;
          case 1: ((long[]) page[i])[ix] = ((Number) values[i]).longValue(); break;
          case 2: ((double[]) page[i])[ix] = ((Number) values[i]).doubleValue(); break;
          case 3:
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

      copy[0] = timestamps.flush();
      for (int i = 1; i < category.length; i++) {
        switch (category[i]) {
          case 0: copy[i] = Arrays.copyOf((float[]) page[i], size); break;
          case 1: copy[i] = Arrays.copyOf((long[]) page[i], size); break;
          case 2: copy[i] = Arrays.copyOf((double[]) page[i], size); break;
          case 3:
            final BytesOutputStream stream = (BytesOutputStream) page[i];
            final byte[] compressed = new byte[Integer.BYTES + LZ4.maxCompressedLength(stream.size())];
            System.arraycopy(Ints.toByteArray(stream.size()), 0, compressed, 0, Integer.BYTES);
            copy[i] = Arrays.copyOf(
                compressed,
                Integer.BYTES + LZ4.compress(stream.toByteArray(), 0, stream.size(), compressed, Integer.BYTES)
            );
            stream.reset();
            break;
          default: copy[i] = Arrays.copyOf((Object[]) page[i], size); break;
        }
      }
      index = 0;
      return retValue = accumulator.accumulate(retValue, new BulkRow(copy));
    }
  }
}
