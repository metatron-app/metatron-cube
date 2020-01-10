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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.ColumnPartProviders;
import io.druid.segment.CompressedVSizeIndexedSupplier;
import io.druid.segment.CompressedVSizeIndexedV3Supplier;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.ColumnPartWriter;
import io.druid.segment.data.CompressedVSizeIntsIndexedSupplier;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.DictionarySketch;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.VSizeIndexed;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.data.WritableSupplier;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;


public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  private static final Logger LOG = new Logger(DictionaryEncodedColumnPartSerde.class);

  private static final int NO_FLAGS = 0;

  enum VERSION
  {
    UNCOMPRESSED_SINGLE_VALUE,  // 0x0
    UNCOMPRESSED_MULTI_VALUE,   // 0x1
    COMPRESSED;                 // 0x2


    public static VERSION fromByte(byte b)
    {
      final VERSION[] values = VERSION.values();
      Preconditions.checkArgument(b < values.length, "Unsupported dictionary column version[%s]", b);
      return values[b];
    }

    public byte asByte()
    {
      return (byte) this.ordinal();
    }
  }

  enum Feature
  {
    MULTI_VALUE,
    MULTI_VALUE_V3,
    NO_DICTIONARY,
    DICTIONARY_SKETCH;

    public static boolean hasAny(int flags, Feature... features)
    {
      for (Feature feature : features) {
        if (feature.isSet(flags)) {
          return true;
        }
      }
      return false;
    }

    public static boolean isSet(int flags, Feature feature) { return feature.isSet(flags); }

    public boolean isSet(int flags) { return (getMask() & flags) != 0; }

    public int getMask() { return (1 << ordinal()); }
  }

  @JsonCreator
  public static DictionaryEncodedColumnPartSerde createDeserializer(
      @Nullable @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @NotNull @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new DictionaryEncodedColumnPartSerde(
        byteOrder,
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(),
        null
    );
  }

  private final ByteOrder byteOrder;
  private final BitmapSerdeFactory bitmapSerdeFactory;
  private final Serializer serializer;

  private DictionaryEncodedColumnPartSerde(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      Serializer serializer
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.serializer = serializer;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private VERSION version = null;
    private int flags = Feature.NO_DICTIONARY.getMask();
    private ColumnPartWriter<String> dictionaryWriter = null;
    private ColumnPartWriter<Pair<String, Integer>> sketchWriter = null;
    private ColumnPartWriter valueWriter = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;
    private ColumnPartWriter<ImmutableBitmap> bitmapIndexWriter = null;
    private ColumnPartWriter<ImmutableRTree> spatialIndexWriter = null;
    private ByteOrder byteOrder = null;

    public SerializerBuilder withDictionary(ColumnPartWriter<String> dictionaryWriter)
    {
      this.dictionaryWriter = dictionaryWriter;
      if (dictionaryWriter == null) {
        flags |= Feature.NO_DICTIONARY.getMask();
      } else {
        flags &= ~Feature.NO_DICTIONARY.getMask();
      }
      return this;
    }

    public SerializerBuilder withDictionarySketch(ColumnPartWriter<Pair<String, Integer>> sketchWriter)
    {
      this.sketchWriter = sketchWriter;
      if (sketchWriter != null) {
        flags |= Feature.DICTIONARY_SKETCH.getMask();
      } else {
        flags &= ~Feature.DICTIONARY_SKETCH.getMask();
      }
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public SerializerBuilder withBitmapIndex(ColumnPartWriter<ImmutableBitmap> bitmapIndexWriter)
    {
      this.bitmapIndexWriter = bitmapIndexWriter;
      return this;
    }

    public SerializerBuilder withSpatialIndex(ColumnPartWriter<ImmutableRTree> spatialIndexWriter)
    {
      this.spatialIndexWriter = spatialIndexWriter;
      return this;
    }

    public SerializerBuilder withByteOrder(ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withValue(ColumnPartWriter valueWriter, boolean hasMultiValue, boolean compressed)
    {
      this.valueWriter = valueWriter;
      if (hasMultiValue) {
        if (compressed) {
          this.version = VERSION.COMPRESSED;
          this.flags |= Feature.MULTI_VALUE_V3.getMask();
        } else {
          this.version = VERSION.UNCOMPRESSED_MULTI_VALUE;
          this.flags |= Feature.MULTI_VALUE.getMask();
        }
      } else {
        if (compressed) {
          this.version = VERSION.COMPRESSED;
        } else {
          this.version = VERSION.UNCOMPRESSED_SINGLE_VALUE;
        }
      }
      return this;
    }

    public DictionaryEncodedColumnPartSerde build()
    {
      return new DictionaryEncodedColumnPartSerde(
          byteOrder,
          bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize() throws IOException
            {
              long size = 1 + // version
                          (version.compareTo(VERSION.COMPRESSED) >= 0
                           ? Ints.BYTES
                           : 0); // flag if version >= compressed
              if (dictionaryWriter != null) {
                size += dictionaryWriter.getSerializedSize();
              }
              if (sketchWriter != null) {
                size += sketchWriter.getSerializedSize();
              }
              if (valueWriter != null) {
                size += valueWriter.getSerializedSize();
              }
              if (bitmapIndexWriter != null) {
                size += bitmapIndexWriter.getSerializedSize();
              }
              if (spatialIndexWriter != null) {
                size += spatialIndexWriter.getSerializedSize();
              }
              return size;
            }

            @Override
            public void writeToChannel(WritableByteChannel channel) throws IOException
            {
              channel.write(ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.compareTo(VERSION.COMPRESSED) >= 0) {
                channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }
              if (dictionaryWriter != null) {
                dictionaryWriter.writeToChannel(channel);
              }
              if (sketchWriter != null) {
                sketchWriter.writeToChannel(channel);
              }
              if (valueWriter != null) {
                valueWriter.writeToChannel(channel);
              }
              if (bitmapIndexWriter != null) {
                bitmapIndexWriter.writeToChannel(channel);
              }
              if (spatialIndexWriter != null) {
                spatialIndexWriter.writeToChannel(channel);
              }
            }

            @Override
            public Map<String, Object> getSerializeStats()
            {
              return null;
            }
          }
      );
    }
  }

  public static LegacySerializerBuilder legacySerializerBuilder()
  {
    return new LegacySerializerBuilder();
  }

  public static class LegacySerializerBuilder
  {
    private VERSION version = null;
    private int flags = NO_FLAGS;
    private GenericIndexed<String> dictionary = null;
    private WritableSupplier<IndexedInts> singleValuedColumn = null;
    private WritableSupplier<IndexedMultivalue<IndexedInts>> multiValuedColumn = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;
    private GenericIndexed<ImmutableBitmap> bitmaps = null;
    private ImmutableRTree spatialIndex = null;
    private ByteOrder byteOrder = null;

    private LegacySerializerBuilder()
    {
    }

    public LegacySerializerBuilder withDictionary(GenericIndexed<String> dictionary)
    {
      this.dictionary = dictionary;
      return this;
    }

    public LegacySerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public LegacySerializerBuilder withBitmaps(GenericIndexed<ImmutableBitmap> bitmaps)
    {
      this.bitmaps = bitmaps;
      return this;
    }

    public LegacySerializerBuilder withSpatialIndex(ImmutableRTree spatialIndex)
    {
      this.spatialIndex = spatialIndex;
      return this;
    }

    public LegacySerializerBuilder withByteOrder(ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public LegacySerializerBuilder withSingleValuedColumn(VSizeIndexedInts singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.UNCOMPRESSED_SINGLE_VALUE;
      this.singleValuedColumn = singleValuedColumn.asWritableSupplier();
      return this;
    }

    public LegacySerializerBuilder withSingleValuedColumn(CompressedVSizeIntsIndexedSupplier singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.COMPRESSED;
      this.singleValuedColumn = singleValuedColumn;
      return this;
    }

    public LegacySerializerBuilder withMultiValuedColumn(VSizeIndexed multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both multiValuedColumn and singleValuedColumn");
      this.version = VERSION.UNCOMPRESSED_MULTI_VALUE;
      this.flags |= Feature.MULTI_VALUE.getMask();
      this.multiValuedColumn = multiValuedColumn.asWritableSupplier();
      return this;
    }

    public LegacySerializerBuilder withMultiValuedColumn(CompressedVSizeIndexedSupplier multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.COMPRESSED;
      this.flags |= Feature.MULTI_VALUE.getMask();
      this.multiValuedColumn = multiValuedColumn;
      return this;
    }

    public DictionaryEncodedColumnPartSerde build()
    {
      Preconditions.checkArgument(
          singleValuedColumn != null ^ multiValuedColumn != null,
          "Exactly one of singleValCol[%s] or multiValCol[%s] must be set",
          singleValuedColumn, multiValuedColumn
      );

      return new DictionaryEncodedColumnPartSerde(
          byteOrder,
          bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize()
            {
              long size = 1 + // version
                          (version.compareTo(VERSION.COMPRESSED) >= 0 ? Ints.BYTES : 0);// flag if version >= compressed

              size += dictionary.getSerializedSize();

              if (Feature.MULTI_VALUE.isSet(flags)) {
                size += multiValuedColumn.getSerializedSize();
              } else {
                size += singleValuedColumn.getSerializedSize();
              }

              size += bitmaps.getSerializedSize();
              if (spatialIndex != null) {
                size += spatialIndex.size() + Ints.BYTES;
              }
              return size;
            }

            @Override
            public void writeToChannel(WritableByteChannel channel) throws IOException
            {
              channel.write(ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.compareTo(VERSION.COMPRESSED) >= 0) {
                channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }

              if (dictionary != null) {
                dictionary.writeToChannel(channel);
              }

              if (Feature.MULTI_VALUE.isSet(flags)) {
                if (multiValuedColumn != null) {
                  multiValuedColumn.writeToChannel(channel);
                }
              } else {
                if (singleValuedColumn != null) {
                  singleValuedColumn.writeToChannel(channel);
                }
              }

              if (bitmaps != null) {
                bitmaps.writeToChannel(channel);
              }

              if (spatialIndex != null) {
                ByteBufferSerializer.writeToChannel(
                    spatialIndex,
                    new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory()),
                    channel
                );
              }
            }

            @Override
            public Map<String, Object> getSerializeStats()
            {
              return null;
            }
          }
      );
    }
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
          ByteBuffer buffer,
          ColumnBuilder builder,
          BitmapSerdeFactory serdeFactory
      )
      {
        final VERSION rVersion = VERSION.fromByte(buffer.get());
        final int rFlags;

        if (rVersion.compareTo(VERSION.COMPRESSED) < 0) {
          rFlags = rVersion.equals(VERSION.UNCOMPRESSED_MULTI_VALUE) ? Feature.MULTI_VALUE.getMask() : NO_FLAGS;
        } else {
          rFlags = buffer.getInt();
        }

        ColumnPartProvider<Dictionary<String>> rDictionary = null;
        if (!Feature.isSet(rFlags, Feature.NO_DICTIONARY)) {
          rDictionary = StringMetricSerde.deserializeDictionary(buffer, ObjectStrategy.STRING_STRATEGY);
        }

        DictionarySketch sketch = null;
        if (Feature.isSet(rFlags, Feature.DICTIONARY_SKETCH)) {
          sketch = DictionarySketch.of(buffer);
        }

        final boolean hasMultipleValues = Feature.hasAny(rFlags, Feature.MULTI_VALUE, Feature.MULTI_VALUE_V3);

        final ColumnPartProvider<IndexedInts> rSingleValuedColumn;
        final ColumnPartProvider<IndexedMultivalue<IndexedInts>> rMultiValuedColumn;

        if (hasMultipleValues) {
          rMultiValuedColumn = readMultiValuedColumn(rVersion, buffer, rFlags);
          rSingleValuedColumn = null;
        } else {
          rSingleValuedColumn = readSingleValuedColumn(rVersion, buffer);
          rMultiValuedColumn = null;
        }

        builder.setType(ValueDesc.STRING)
               .setHasMultipleValues(hasMultipleValues)
               .setDictionaryEncodedColumn(
                   new DictionaryEncodedColumnSupplier(
                       rDictionary,
                       rSingleValuedColumn,
                       rMultiValuedColumn,
                       sketch
                   )
               );

        final GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
            buffer, bitmapSerdeFactory.getObjectStrategy()
        );
        builder.setBitmapIndex(
            new BitmapIndexColumnPartSupplier(
                bitmapSerdeFactory.getBitmapFactory(),
                rBitmaps,
                rDictionary
            )
        );

        if (buffer.hasRemaining()) {
          ImmutableRTree rSpatialIndex = ByteBufferSerializer.read(
              buffer, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
          );
          builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(rSpatialIndex));
        }
      }


      private ColumnPartProvider<IndexedInts> readSingleValuedColumn(VERSION version, ByteBuffer buffer)
      {
        switch (version) {
          case UNCOMPRESSED_SINGLE_VALUE:
            return ColumnPartProviders.ofInstance(VSizeIndexedInts.readFromByteBuffer(buffer));
          case COMPRESSED:
            return CompressedVSizeIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);
        }
        throw new IAE("Unsupported single-value version[%s]", version);
      }

      private ColumnPartProvider<IndexedMultivalue<IndexedInts>> readMultiValuedColumn(
          VERSION version, ByteBuffer buffer, int flags
      )
      {
        switch (version) {
          case UNCOMPRESSED_MULTI_VALUE:
            return ColumnPartProviders.ofInstance(VSizeIndexed.readFromByteBuffer(buffer));
          case COMPRESSED:
            if (Feature.MULTI_VALUE.isSet(flags)) {
              return CompressedVSizeIndexedSupplier.fromByteBuffer(buffer, byteOrder);
            } else if (Feature.MULTI_VALUE_V3.isSet(flags)) {
              return CompressedVSizeIndexedV3Supplier.fromByteBuffer(buffer, byteOrder);
            } else {
              throw new IAE("Unrecognized multi-value flag[%d]", flags);
            }
        }
        throw new IAE("Unsupported multi-value version[%s]", version);
      }
    };
  }
}
