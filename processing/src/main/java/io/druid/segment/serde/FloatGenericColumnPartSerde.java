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
import io.druid.segment.column.FloatScanner;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.IntDoubleConsumer;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedFloatBufferObjectStrategy;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import it.unimi.dsi.fastutil.ints.Int2FloatFunction;
import org.apache.commons.lang.mutable.MutableFloat;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.util.stream.DoubleStream;

/**
 */
public class FloatGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static FloatGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new FloatGenericColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  private final Serializer serializer;

  public FloatGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
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

        builder.setType(ValueDesc.FLOAT)
               .setHasMultipleValues(false);

        if (compression == CompressionStrategy.NONE) {
          final FloatBuffer bufferToUse = ByteBufferSerializer.prepareForRead(buffer, Float.BYTES * numRows)
                                                              .asFloatBuffer();
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
                     (long) Float.BYTES * numRows;
            }

            @Override
            public Class<? extends GenericColumn> provides()
            {
              return GenericColumn.FloatType.class;
            }

            @Override
            public GenericColumn get()
            {
              return new GenericColumn.FloatType()
              {
                private final ImmutableBitmap bitmap = nulls.get();

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
                  return bitmap;
                }

                @Override
                public Float getValue(int rowNum)
                {
                  return bitmap.get(rowNum) ? null : bufferToUse.get(rowNum);
                }

                @Override
                public boolean getFloat(int rowNum, MutableFloat handover)
                {
                  if (bitmap.get(rowNum)) {
                    return false;
                  } else {
                    handover.setValue(bufferToUse.get(rowNum));
                    return true;
                  }
                }

                @Override
                public void scan(IntIterator iterator, FloatScanner scanner)
                {
                  final IntIterator it = IntIterators.except(iterator, bitmap, size());
                  final Int2FloatFunction supplier = x -> bufferToUse.get(x);
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
                public void consume(IntIterator iterator, IntDoubleConsumer consumer)
                {
                  scan(iterator, consumer.asFloatScanner());
                }

                @Override
                public DoubleStream stream(IntIterator iterator)
                {
                  return IntIterators.filteredStream(iterator, bitmap, size()).mapToDouble(x -> bufferToUse.get(x));
                }
              };
            }
          });
        } else {
          CompressedFloatBufferObjectStrategy strategy =
              CompressedFloatBufferObjectStrategy.getBufferForOrder(byteOrder, compression, sizePer);
          CompressedFloatsIndexedSupplier column = new CompressedFloatsIndexedSupplier(
              numRows, sizePer, GenericIndexed.read(buffer, strategy), compression
          );
          builder.setGenericColumn(
              new FloatGenericColumnSupplier(column, ComplexMetrics.readBitmap(buffer, serdeFactory))
          );
        }
      }
    };
  }
}
