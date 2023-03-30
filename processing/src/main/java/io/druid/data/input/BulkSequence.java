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
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.guava.YieldingAccumulator;
import io.druid.common.guava.YieldingSequenceBase;
import io.druid.common.utils.FrontCoding;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.data.VLongUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.RowSignature;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Remove type information & apply compresssion if possible
 */
public class BulkSequence extends YieldingSequenceBase<BulkRow>
{
  private static final Logger LOG = new Logger(BulkSequence.class);

  public static Sequence<BulkRow> fromRow(Sequence<Row> sequence, RowSignature schema, int limit, int sorted)
  {
    return fromArray(Sequences.map(sequence, CompactRow.UNWRAP), schema, limit, sorted);
  }

  public static Sequence<BulkRow> fromArray(Sequence<Object[]> sequence, RowSignature schema)
  {
    return fromArray(sequence, schema, DEFAULT_PAGE_SIZE, -1);
  }

  public static Sequence<BulkRow> fromArray(Sequence<Object[]> sequence, RowSignature schema, int limit, int sorted)
  {
    return new BulkSequence(sequence, schema, limit <= 0 ? DEFAULT_PAGE_SIZE : Math.min(MAX_PAGE_SIZE, limit), -1);   // disabled
  }

  private static final int DEFAULT_PAGE_SIZE = 1024 << 1;
  private static final int MAX_PAGE_SIZE = 1024 << 3;

  private final Sequence<Object[]> sequence;
  private final RowSignature schema;
  private final int[] category;
  private final Object[] page;
  private final int max;
  private final int sorted;
  private final StringWriter[] writers;

  BulkSequence(Sequence<Object[]> sequence, RowSignature schema, int max, int sorted)
  {
    Preconditions.checkArgument(max > 0 && max < 0xffff);
    this.max = max;
    this.sequence = sequence;
    this.schema = schema;
    this.category = new int[schema.size()];
    this.page = new Object[schema.size()];
    this.writers = new StringWriter[schema.size()];
    final List<ValueDesc> types = schema.getColumnTypes();
    for (int i = 0; i < types.size(); i++) {
      final ValueDesc valueDesc = types.get(i).unwrapDimension();
      switch (valueDesc.type()) {
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
          writers[i] = writer(i == sorted);
          break;
        default:
          category[i] = 6;
          page[i] = new Object[max];
      }
    }
    this.sorted = sorted >= 0 && category[sorted] == 5 ? sorted : -1;
  }

  @Override
  public List<String> columns()
  {
    return schema.getColumnNames();
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
          case 1: ((Float[]) page[i])[ix] = values[i] == null ? null : ((Number) values[i]).floatValue(); continue;
          case 2: ((Long[]) page[i])[ix] = values[i] == null ? null : ((Number) values[i]).longValue(); continue;
          case 3: ((Double[]) page[i])[ix] = values[i] == null ? null : ((Number) values[i]).doubleValue(); continue;
          case 4: ((Boolean[]) page[i])[ix] = Rows.parseBoolean(values[i]); continue;
          case 5: writers[i].accept((BytesOutputStream) page[i], values[i]); continue;
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
          case 5: copy[i] = writers[i].next((BytesOutputStream) page[i]);continue;
          case 6: copy[i] = Arrays.copyOf((Object[]) page[i], size); continue;
          default:
            throw new ISE("invalid type %d", category[i]);
        }
      }
      index = 0;
      return retValue = accumulator.accumulate(retValue, toBulkRow(size, category, copy));
    }
  }

  private BulkRow toBulkRow(int size, int[] category, Object[] copy)
  {
    return new BulkRow(size, category, copy, sorted);
  }

  private static interface StringWriter extends BiConsumer<BytesOutputStream, Object>
  {
    default byte[] next(BytesOutputStream o)
    {
      final byte[] array = o.toByteArray();
      o.clear();
      return array;
    }
  }

  private static StringWriter writer(boolean sorted)
  {
    if (!sorted) {
      return (o, v) -> o.writeVarSizeUTF(v);
    }
    return new StringWriter()
    {
      private int size;
      private BytesRef prev;

      @Override
      public void accept(BytesOutputStream o, Object v)
      {
        BytesRef current = StringUtils.stringAsRef(v);
        if (prev == null) {
          o.writeVarSizeBytes(current);
        } else {
          int common = FrontCoding.commonPrefix(prev, current);
          o.writeUnsignedVarInt(common);
          o.writeUnsignedVarInt(current.length - common);
          o.write(current, common, current.length - common);
        }
        size += current.length + VLongUtils.sizeOfUnsignedVarInt(current.length);
        prev = current;
      }

      @Override
      public byte[] next(BytesOutputStream o)
      {
        prev = null;
        return StringWriter.super.next(o);
      }
    };
  }

  @Override
  public String toString()
  {
    return "BulkSequence{" +
           "category=" + Arrays.toString(category) +
           '}';
  }
}
