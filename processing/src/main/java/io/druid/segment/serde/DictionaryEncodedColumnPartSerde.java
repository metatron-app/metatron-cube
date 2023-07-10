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
import io.druid.data.VLongUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.ColumnPartProviders;
import io.druid.segment.CompressedVIntsSupplierV3;
import io.druid.segment.CompressedVintsSupplier;
import io.druid.segment.IndexIO;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedVintsReader;
import io.druid.segment.data.CumulativeBitmapWriter;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedRTree;
import io.druid.segment.data.IntValues;
import io.druid.segment.data.IntsValues;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.VintValues;
import io.druid.segment.data.VintsValues;
import io.druid.segment.data.WritableSupplier;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  private static final Logger LOG = new Logger(DictionaryEncodedColumnPartSerde.class);

  private static final int NO_FLAGS = 0;

  // what the..
  enum VERSION
  {
    LEGACY_SINGLE_VALUE,  // 0x0
    LEGACY_MULTI_VALUE,   // 0x1
    LEGACY_COMPRESSED;    // 0x2

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
    MULTI_VALUE(0),
    MULTI_VALUE_V3(1),
    NO_DICTIONARY(2),
    RESERVED(3),
    CUMULATIVE_BITMAPS(4),
    UNCOMPRESSED(5),     // override LEGACY_COMPRESSED
    SPATIAL_INDEX(6)     // technically, it's not backward compatible with index v8 but who cares?
    ;

    private final int mask;

    Feature(int ordinal)
    {
      mask = 1 << ordinal;
    }

    public static boolean hasAny(int flags, Feature... features)
    {
      for (Feature feature : features) {
        if (feature.isSet(flags)) {
          return true;
        }
      }
      return false;
    }

    public boolean isSet(int flags) { return (mask & flags) != 0; }

    public int getMask() { return mask; }

    public int set(int flags, boolean bool)
    {
      if (bool) {
        flags |= mask;
      } else {
        flags &= ~mask;
      }
      return flags;
    }
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
    private int flags = Feature.NO_DICTIONARY.set(NO_FLAGS, true);

    private ColumnPartSerde.Serializer dictionary;
    private ColumnPartSerde.Serializer values;
    private ColumnPartSerde.Serializer bitmaps;
    private ColumnPartSerde.Serializer rindices;

    public SerializerBuilder withDictionary(ColumnPartSerde.Serializer dictionary)
    {
      this.dictionary = dictionary;
      this.flags = Feature.NO_DICTIONARY.set(flags, dictionary == null);
      return this;
    }

    public SerializerBuilder withBitmapIndex(ColumnPartSerde.Serializer bitmaps)
    {
      this.bitmaps = bitmaps;
      this.flags = Feature.CUMULATIVE_BITMAPS.set(flags, bitmaps instanceof CumulativeBitmapWriter);
      return this;
    }

    public SerializerBuilder withSpatialIndex(ColumnPartSerde.Serializer rindices)
    {
      this.rindices = rindices;
      this.flags = Feature.SPATIAL_INDEX.set(flags, rindices != null);
      return this;
    }

    public SerializerBuilder withValue(ColumnPartSerde.Serializer valueWriter, boolean hasMultiValue, boolean compressed)
    {
      this.values = valueWriter;
      if (hasMultiValue && compressed) {
        this.flags = Feature.MULTI_VALUE_V3.set(flags, true);
      }
      this.flags = Feature.UNCOMPRESSED.set(flags, !compressed);
      return this;
    }

    public DictionaryEncodedColumnPartSerde build(BitmapSerdeFactory serdeFactory)
    {
      return new DictionaryEncodedColumnPartSerde(
          IndexIO.BYTE_ORDER,
          serdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize()
            {
              long size = Byte.BYTES + Integer.BYTES;
              if (dictionary != null) {
                size += dictionary.getSerializedSize();
              }
              if (values != null) {
                size += values.getSerializedSize();
              }
              if (bitmaps != null) {
                size += bitmaps.getSerializedSize();
              }
              if (rindices != null) {
                size += rindices.getSerializedSize();
              }
              return size;
            }

            @Override
            public long writeToChannel(WritableByteChannel channel) throws IOException
            {
              long written = channel.write(ByteBuffer.wrap(new byte[]{VERSION.LEGACY_COMPRESSED.asByte()}));
              written += channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));

              if (dictionary != null) {
                written += dictionary.writeToChannel(channel);
              }
              if (values != null) {
                written += values.writeToChannel(channel);
              }
              if (bitmaps != null) {
                written += bitmaps.writeToChannel(channel);
              }
              if (rindices != null) {
                written += rindices.writeToChannel(channel);
              }
              return written;
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
    private WritableSupplier<IntValues> singleValuedColumn = null;
    private WritableSupplier<IntsValues> multiValuedColumn = null;
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
      this.flags = Feature.SPATIAL_INDEX.set(flags, spatialIndex != null);
      return this;
    }

    public LegacySerializerBuilder withByteOrder(ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public LegacySerializerBuilder withSingleValuedColumn(VintValues singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.LEGACY_SINGLE_VALUE;
      this.singleValuedColumn = singleValuedColumn;
      return this;
    }

    public LegacySerializerBuilder withSingleValuedColumn(CompressedVintsReader singleValuedColumn)
    {
      Preconditions.checkState(multiValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.LEGACY_COMPRESSED;
      this.singleValuedColumn = singleValuedColumn;
      return this;
    }

    public LegacySerializerBuilder withMultiValuedColumn(VintsValues multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both multiValuedColumn and singleValuedColumn");
      this.version = VERSION.LEGACY_MULTI_VALUE;
      this.multiValuedColumn = multiValuedColumn;
      return this;
    }

    public LegacySerializerBuilder withMultiValuedColumn(CompressedVintsSupplier multiValuedColumn)
    {
      Preconditions.checkState(singleValuedColumn == null, "Cannot set both singleValuedColumn and multiValuedColumn");
      this.version = VERSION.LEGACY_COMPRESSED;
      this.flags = Feature.MULTI_VALUE.set(flags, true);
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
                          (version.compareTo(VERSION.LEGACY_COMPRESSED) >= 0 ? Integer.BYTES : 0);// flag if version >= compressed

              size += dictionary.getSerializedSize();

              if (version == VERSION.LEGACY_MULTI_VALUE || Feature.MULTI_VALUE.isSet(flags)) {
                size += multiValuedColumn.getSerializedSize();
              } else {
                size += singleValuedColumn.getSerializedSize();
              }

              size += bitmaps.getSerializedSize();
              if (spatialIndex != null) {
                size += spatialIndex.size() + Integer.BYTES;
              }
              return size;
            }

            @Override
            public long writeToChannel(WritableByteChannel channel) throws IOException
            {
              long written = channel.write(ByteBuffer.wrap(new byte[]{version.asByte()}));
              if (version.compareTo(VERSION.LEGACY_COMPRESSED) >= 0) {
                written += channel.write(ByteBuffer.wrap(Ints.toByteArray(flags)));
              }

              if (dictionary != null) {
                written += dictionary.writeToChannel(channel);
              }

              if (version == VERSION.LEGACY_MULTI_VALUE || Feature.MULTI_VALUE.isSet(flags)) {
                if (multiValuedColumn != null) {
                  written += multiValuedColumn.writeToChannel(channel);
                }
              } else {
                if (singleValuedColumn != null) {
                  written += singleValuedColumn.writeToChannel(channel);
                }
              }

              if (bitmaps != null) {
                written += bitmaps.writeToChannel(channel);
              }

              if (spatialIndex != null) {
                written += ByteBufferSerializer.writeToChannel(
                    spatialIndex,
                    new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory()),
                    channel
                );
              }
              return written;
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
      public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
      {
        final VERSION rVersion = VERSION.fromByte(buffer.get());
        final int rFlags;

        if (rVersion.compareTo(VERSION.LEGACY_COMPRESSED) < 0) {
          rFlags = rVersion.equals(VERSION.LEGACY_MULTI_VALUE) ? Feature.MULTI_VALUE.set(NO_FLAGS, true) : NO_FLAGS;
        } else {
          rFlags = buffer.getInt();
        }

        builder.setType(ValueDesc.STRING);

        if (!Feature.NO_DICTIONARY.isSet(rFlags)) {
          builder.setDictionary(readDictionary(buffer));
        }

        final boolean hasMultipleValues = Feature.hasAny(rFlags, Feature.MULTI_VALUE, Feature.MULTI_VALUE_V3);
        final boolean compressed = rVersion == VERSION.LEGACY_COMPRESSED && !Feature.UNCOMPRESSED.isSet(rFlags);

        if (hasMultipleValues) {
          builder.setHasMultipleValues(hasMultipleValues)
                 .setMultiValuedColumn(readMultiValuedColumn(buffer, rFlags, compressed));
        } else {
          builder.setSingleValuedColumn(readSingleValuedColumn(buffer, compressed));
        }

        ObjectStrategy<ImmutableBitmap> strategy = bitmapSerdeFactory.getObjectStrategy();
        GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(buffer, strategy);

        int[] cumulativeThresholds = null;
        GenericIndexed<ImmutableBitmap> cumulativeBitmaps = null;
        if (Feature.CUMULATIVE_BITMAPS.isSet(rFlags)) {
          cumulativeThresholds = new int[VLongUtils.readVInt(buffer)];
          for (int i = 0; i < cumulativeThresholds.length; i++) {
            cumulativeThresholds[i] = VLongUtils.readVInt(buffer);
          }
          cumulativeBitmaps = GenericIndexed.read(buffer, strategy);
        }

        builder.setBitmapIndex(
            new BitmapIndexColumnPartSupplier(
                bitmapSerdeFactory.getBitmapFactory(),
                rBitmaps,
                builder.getDictionary(),
                cumulativeThresholds,
                cumulativeBitmaps
            )
        );

        if (Feature.SPATIAL_INDEX.isSet(rFlags)) {
          ImmutableRTree rSpatialIndex = ByteBufferSerializer.read(
              buffer, new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
          );
          builder.setSpatialIndex(new SpatialIndexColumnPartSupplier(rSpatialIndex));
        }
      }

      private ColumnPartProvider<Dictionary<String>> readDictionary(ByteBuffer buffer)
      {
        final byte versionFromBuffer = buffer.get();
        if (versionFromBuffer == GenericIndexed.version) {
          return GenericIndexed.readIndex(buffer, ObjectStrategy.STRING_STRATEGY).asColumnPartProvider();
        } else {
          throw new IAE("Unknown version[%s]", versionFromBuffer);
        }
      }

      private ColumnPartProvider<IntValues> readSingleValuedColumn(ByteBuffer buffer, boolean compressed)
      {
        if (!compressed) {
          return ColumnPartProviders.with(VintValues.readFromByteBuffer(buffer));
        } else {
          return CompressedVintsReader.from(buffer, byteOrder);
        }
      }

      private ColumnPartProvider<IntsValues> readMultiValuedColumn(
          ByteBuffer buffer, int flags, boolean compressed
      )
      {
        if (!compressed) {
          return ColumnPartProviders.with(VintsValues.readFromByteBuffer(buffer));
        } else if (Feature.MULTI_VALUE.isSet(flags)) {
          return CompressedVintsSupplier.from(buffer, byteOrder);
        } else if (Feature.MULTI_VALUE_V3.isSet(flags)) {
          return CompressedVIntsSupplierV3.from(buffer, byteOrder);
        } else {
          throw new IAE("Unrecognized multi-value flag[%d]", flags);
        }
      }
    };
  }
}
