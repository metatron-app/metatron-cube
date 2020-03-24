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
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.YieldingSequenceBase;
import io.druid.query.RowSignature;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Remove type information & apply compresssion if possible
 */
public class BulkSequence extends YieldingSequenceBase<BulkRow>
{
  public static Sequence<BulkRow> fromRow(Sequence<Row> sequence, RowSignature schema)
  {
    return fromArray(Sequences.map(sequence, CompactRow.UNWRAP), schema);
  }

  public static Sequence<BulkRow> fromArray(Sequence<Object[]> sequence, RowSignature schema)
  {
    return new BulkSequence(sequence, schema.getColumnTypes());
  }

  private static final int DEFAULT_PAGE_SIZE = 1024;

  private final Sequence<Object[]> sequence;
  private final int[] category;
  private final Object[] page;
  private final int max;

  public BulkSequence(Sequence<Object[]> sequence, List<ValueDesc> types)
  {
    this(sequence, types, DEFAULT_PAGE_SIZE);
  }

  public BulkSequence(Sequence<Object[]> sequence, List<ValueDesc> types, final int max)
  {
    Preconditions.checkArgument(max < 0xffff);    // see TimestampRLE
    this.max = max;
    this.sequence = sequence;
    this.category = new int[types.size()];
    this.page = new Object[types.size()];
    for (int i = 0; i < types.size(); i++) {
      final ValueDesc valueDesc = types.get(i);
      switch (valueDesc.isDimension() ? ValueDesc.typeOfDimension(valueDesc) : valueDesc.type()) {
        case FLOAT:
          category[i] = 1;
          page[i] = new Float[max];
          break;
        case LONG:
          category[i] = 2;
          page[i] = new Long[max];
          break;
        case DOUBLE:
          category[i] = 3;
          page[i] = new Double[max];
          break;
        case BOOLEAN:
          category[i] = 4;
          page[i] = new Boolean[max];
          break;
        case STRING:
          category[i] = 5;
          page[i] = new BytesOutputStream(4096);
          break;
        default:
          category[i] = 6;
          page[i] = new Object[max];
      }
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue,
      YieldingAccumulator<OutType, BulkRow> accumulator
  )
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
        if (accumulator.index > 0) {
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
        return accumulator.index == 0 && (yielder == null || yielder.isDone());
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
    private final YieldingAccumulator<OutType, BulkRow> accumulator;

    private int index;
    private OutType retValue;

    public BulkYieldingAccumulator(OutType retValue, YieldingAccumulator<OutType, BulkRow> accumulator)
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
        switch (category[i]) {
          case 1: ((Float[]) page[i])[ix] = Rows.parseFloat(values[i]); continue;
          case 2: ((Long[]) page[i])[ix] = Rows.parseLong(values[i]); continue;
          case 3: ((Double[]) page[i])[ix] = Rows.parseDouble(values[i]); continue;
          case 4: ((Boolean[]) page[i])[ix] = Rows.parseBoolean(values[i]); continue;
          case 5: ((BytesOutputStream) page[i]).writeVarSizeBytes(StringUtils.toUtf8WithNullToEmpty(values[i])); continue;
          case 6: ((Object[]) page[i])[ix] = values[i]; continue;
          default:
            throw new ISE("invalid type %d", category[i]);
        }
      }
      return index < max ? prevValue : asBulkRow();
    }

    private OutType asBulkRow()
    {
      final int size = index;
      final Object[] copy = new Object[page.length];

      // unnecessary copy ?
      for (int i = 0; i < category.length; i++) {
        switch (category[i]) {
          case 1: copy[i] = Arrays.copyOf((Float[]) page[i], size); continue;
          case 2: copy[i] = Arrays.copyOf((Long[]) page[i], size); continue;
          case 3: copy[i] = Arrays.copyOf((Double[]) page[i], size); continue;
          case 4: copy[i] = Arrays.copyOf((Boolean[]) page[i], size); continue;
          case 5:
            final BytesOutputStream stream = (BytesOutputStream) page[i];
            copy[i] = stream.toByteArray();
            stream.clear();
            continue;
          case 6: copy[i] = Arrays.copyOf((Object[]) page[i], size); continue;
          default:
            throw new ISE("invalid type %d", category[i]);
        }
      }
      index = 0;
      return retValue = accumulator.accumulate(retValue, toBulkRow(size, category, copy));
    }
  }

  protected BulkRow toBulkRow(int size, int[] category, Object[] copy)
  {
    return new BulkRow(size, category, copy);
  }
}
