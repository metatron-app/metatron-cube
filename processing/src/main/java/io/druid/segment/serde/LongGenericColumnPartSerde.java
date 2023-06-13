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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.bitmap.IntIterators;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IntLongConsumer;
import io.druid.segment.column.LongScanner;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedLongBufferObjectStrategy;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import org.apache.commons.lang.mutable.MutableLong;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.stream.LongStream;

/**
 */
public class LongGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static LongGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new LongGenericColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  private final Serializer serializer;

  public LongGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(
          final ByteBuffer buffer,
          final ColumnBuilder builder,
          final BitmapSerdeFactory serdeFactory
      )
      {
        final byte versionFromBuffer = buffer.get();
        final int numRows = buffer.getInt();
        final int sizePer = buffer.getInt();

        final CompressionStrategy compression;
        if (versionFromBuffer == ColumnPartSerde.WITH_COMPRESSION_ID) {
          compression = CompressedObjectStrategy.forId(buffer.get());
        } else if (versionFromBuffer == ColumnPartSerde.LZF_FIXED) {
          compression = CompressionStrategy.LZF;
        } else {
          throw new IAE("Unknown version[%s]", versionFromBuffer);
        }

        builder.setType(ValueDesc.LONG)
               .setHasMultipleValues(false);

        if (compression == CompressionStrategy.NONE) {
          final LongBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer, Long.BYTES * numRows)
                                                             .asLongBuffer();
          final Supplier<ImmutableBitmap> nulls = ComplexMetrics.readBitmap(buffer, serdeFactory);
          builder.setGenericColumn(new ColumnPartProvider<GenericColumn>()
          {
            @Override
            public int numRows()
            {
              return numRows;
            }

            @Override
            public long getSerializedSize()
            {
              return 1 +              // version
                     Integer.BYTES +  // elements num
                     Integer.BYTES +  // sizePer
                     1 +              // compression id
                     (long) Long.BYTES * numRows;
            }

            @Override
            public Class<? extends GenericColumn> provides()
            {
              return GenericColumn.LongType.class;
            }

            @Override
            public GenericColumn get()
            {
              if (builder.isTimestamp()) {
                return new LongTypeGenericColumn.Timestamp(numRows, bufferToUse, nulls.get())
                {
                  @Override
                  public long timestamp(int offset) {return buffer.get(offset);}
                };
              }
              return new LongTypeGenericColumn(numRows, bufferToUse, nulls.get());
            }
          });
        } else {
          CompressedLongBufferObjectStrategy strategy =
              CompressedLongBufferObjectStrategy.getBufferForOrder(byteOrder, compression, sizePer);
          CompressedLongsIndexedSupplier column = new CompressedLongsIndexedSupplier(
              numRows, sizePer, GenericIndexed.read(buffer, strategy), compression
          );
          Supplier<ImmutableBitmap> nulls = ComplexMetrics.readBitmap(buffer, serdeFactory);
          builder.setGenericColumn(
              new LongGenericColumnSupplier(column, nulls, builder.isTimestamp())
          );
        }
      }
    };
  }

  private static class LongTypeGenericColumn extends GenericColumn.LongType
  {
    private final int numRows;
    private final LongBuffer buffer;
    private final ImmutableBitmap nulls;

    private LongTypeGenericColumn(int numRows, LongBuffer buffer, ImmutableBitmap nulls)
    {
      this.numRows = numRows;
      this.buffer = buffer;
      this.nulls = nulls;
    }

    @Override
    public CompressionStrategy compressionType()
    {
      return CompressionStrategy.NONE;
    }

    @Override
    public int size()
    {
      return numRows;
    }

    @Override
    public ImmutableBitmap getNulls()
    {
      return nulls;
    }

    @Override
    public Long getValue(int rowNum)
    {
      return nulls.get(rowNum) ? null : buffer.get(rowNum);
    }

    @Override
    public boolean getLong(int rowNum, MutableLong handover)
    {
      if (nulls.get(rowNum)) {
        return false;
      } else {
        handover.setValue(buffer.get(rowNum));
        return true;
      }
    }

    @Override
    public void scan(IntIterator iterator, LongScanner scanner)
    {
      final IntIterator it = IntIterators.except(iterator, nulls, size());
      final Int2LongFunction supplier = x -> buffer.get(x);
      if (it == null) {
        for (int i = 0; i < numRows; i++) {
          scanner.apply(i, supplier);
        }
      } else {
        while (it.hasNext()) {
          scanner.apply(it.next(), supplier);
        }
      }
    }

    @Override
    public void consume(IntIterator iterator, IntLongConsumer consumer)
    {
      final IntIterator it = IntIterators.except(iterator, nulls, size());
      if (it == null) {
        for (int i = 0; i < numRows; i++) {
          consumer.apply(i, buffer.get(i));
        }
      } else {
        while (it.hasNext()) {
          int ix = it.next();
          consumer.apply(ix, buffer.get(ix));
        }
      }
    }

    @Override
    public LongStream stream(IntIterator iterator)
    {
      return IntIterators.filteredStream(iterator, nulls, size()).mapToLong(x -> buffer.get(x));
    }

    private static abstract class Timestamp extends LongTypeGenericColumn implements TimestampType
    {
      private Timestamp(int numRows, LongBuffer buffer, ImmutableBitmap nulls)
      {
        super(numRows, buffer, nulls);
      }
    }
  }
}
