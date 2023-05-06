/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
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

package io.druid.data.input;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.FrontCoding;
import io.druid.data.UTF8Bytes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.IntFunction;

public class BulkRow extends AbstractRow
{
  private static final Logger LOG = new Logger(BulkRow.class);

  private static final LZ4Compressor LZ4_COMP = LZ4Factory.fastestInstance().fastCompressor();
  private static final LZ4FastDecompressor LZ4_DECOMP = LZ4Factory.fastestInstance().fastDecompressor();

//  private static final ZstdCompressor LZ4_COMP = new ZstdCompressor();
//  private static final ZstdDecompressor LZ4_DECOMP = new ZstdDecompressor();

  private static final ThreadLocal<BytesOutputStream> BUFFER = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  private static final ThreadLocal<BytesOutputStream> SCRATCH = new ThreadLocal<BytesOutputStream>()
  {
    @Override
    protected BytesOutputStream initialValue()
    {
      return new BytesOutputStream();
    }
  };

  public static final TypeReference<BulkRow> TYPE_REFERENCE = new TypeReference<BulkRow>()
  {
  };

  private static void compressTo(final BytesOutputStream scratch, final BytesOutputStream output)
  {
    compressTo(scratch.unwrap(), scratch.size(), output);
  }

  private static void compressTo(final byte[] scratch, final BytesOutputStream output)
  {
    compressTo(scratch, scratch.length, output);
  }

  private static void compressTo(final byte[] scratch, final int length, final BytesOutputStream output)
  {
    final byte[] compressed = new byte[Integer.BYTES + LZ4_COMP.maxCompressedLength(length)];
    System.arraycopy(Ints.toByteArray(length), 0, compressed, 0, Integer.BYTES);
    output.writeVarSizeBytes(new BytesRef(
        compressed, 0, Integer.BYTES + LZ4_COMP.compress(scratch, 0, length, compressed, Integer.BYTES)
    ));
  }

  public static final JsonSerializer<BulkRow> SERIALIZER = new JsonSerializer<BulkRow>()
  {
    @Override
    public void serializeWithType(BulkRow bulk, JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer)
        throws IOException
    {
      typeSer.writeTypePrefixForObject(bulk, jgen);   // see mixin in AggregatorsModule
      jgen.writeObjectField("s", bulk.encoded);
      jgen.writeFieldName("v");
      serialize(bulk, jgen, provider);
      typeSer.writeTypeSuffixForObject(bulk, jgen);
    }

    @Override
    public void serialize(BulkRow bulk, JsonGenerator jgen, SerializerProvider provider) throws IOException
    {
      final BytesOutputStream output = BUFFER.get();
      final JsonFactory factory = Preconditions.checkNotNull(jgen.getCodec()).getFactory();

      output.clear();
      output.writeUnsignedVarInt(bulk.count);
      output.writeUnsignedVarInt(bulk.category.length);

      for (int i = 0; i < bulk.category.length; i++) {
        output.writeUnsignedVarInt(bulk.category[i]);
      }

      final BytesOutputStream scratch = SCRATCH.get();
      for (int i = 0; i < bulk.category.length; i++) {
        scratch.clear();
        switch (bulk.category[i]) {
          case 1:
            final float[] floats = (float[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              scratch.writeFloat(floats[x]);
            }
            compressTo(writeNulls(bulk.nulls[i], scratch), output);
            continue;
          case 2:
            final long[] longs = (long[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              scratch.writeVarLong(longs[x]);
            }
            compressTo(writeNulls(bulk.nulls[i], scratch), output);
            continue;
          case 3:
            final double[] doubles = (double[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              scratch.writeDouble(doubles[x]);
            }
            compressTo(writeNulls(bulk.nulls[i], scratch), output);
            continue;
          case 4:
            final boolean[] booleans = (boolean[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              scratch.writeBoolean(booleans[x]);
            }
            compressTo(writeNulls(bulk.nulls[i], scratch), output);
            continue;
          case 5:
            compressTo((byte[]) bulk.values[i], output);
            continue;
          case 6:
            final JsonGenerator hack = factory.createGenerator(scratch);
            hack.writeObject(bulk.values[i]);
            hack.flush();
            compressTo(scratch, output);
            continue;
          default:
            throw new ISE("invalid type %d", bulk.category[i]);
        }
      }
      jgen.writeBinary(output.unwrap(), 0, output.size());
    }
  };

  private static BytesOutputStream writeNulls(final BitSet nulls, final BytesOutputStream scratch)
  {
    if (!nulls.isEmpty()) {
      scratch.writeBoolean(true);
      scratch.writeVarSizeBytes(nulls.toByteArray());
    } else {
      scratch.writeBoolean(false);
    }
    return scratch;
  }

  // todo : keep primitive array and null bitset
  public static final JsonDeserializer<BulkRow> DESERIALIZER = new JsonDeserializer<BulkRow>()
  {
    @Override
    public BulkRow deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
    {
      Preconditions.checkArgument(jp.getCurrentToken() == JsonToken.FIELD_NAME);
      Preconditions.checkArgument("s".equals(jp.getText()));
      Preconditions.checkArgument(jp.nextToken() == JsonToken.VALUE_NUMBER_INT);
      final int encoded = jp.getIntValue();

      Preconditions.checkArgument(jp.nextToken() == JsonToken.FIELD_NAME);
      Preconditions.checkArgument("v".equals(jp.getText()));
      jp.nextToken();

      final BytesInputStream input = new BytesInputStream(jp.getBinaryValue());
      final JsonFactory factory = Preconditions.checkNotNull(jp.getCodec()).getFactory();

      final int count = input.readUnsignedVarInt();
      final int[] category = new int[input.readUnsignedVarInt()];
      for (int i = 0; i < category.length; i++) {
        category[i] = input.readUnsignedVarInt();
      }
      final Object[] values = new Object[category.length];
      final BitSet[] nulls = new BitSet[category.length];
      final BytesOutputStream scratch = SCRATCH.get();

      int offset = 0;
      for (int i = 0; i < category.length; i++) {
        final byte[] array = input.readVarSizeBytes();
        final int destLen = Ints.fromByteArray(array);
        final BytesInputStream decompressed;
        if (category[i] == 5) {
          // need copy
          decompressed = new BytesInputStream(LZ4_DECOMP.decompress(array, Integer.BYTES, destLen));
        } else {
          scratch.ensureCapacity(destLen);
          scratch.clear();
          LZ4_DECOMP.decompress(array, Integer.BYTES, scratch.unwrap(), 0, destLen);
          decompressed = new BytesInputStream(scratch.unwrap());
        }
        switch (category[i]) {
          case 1:
            final float[] floats = new float[count];
            for (int x = 0; x < count; x++) {
              floats[x] = decompressed.readFloat();
            }
            if (decompressed.readBoolean()) {
              nulls[i] = BitSet.valueOf(decompressed.readVarSizeBytes());
            }
            values[i] = floats;
            continue;
          case 2:
            final long[] longs = new long[count];
            for (int x = 0; x < count; x++) {
              longs[x] = decompressed.readVarLong();
            }
            if (decompressed.readBoolean()) {
              nulls[i] = BitSet.valueOf(decompressed.readVarSizeBytes());
            }
            values[i] = longs;
            continue;
          case 3:
            final double[] doubles = new double[count];
            for (int x = 0; x < count; x++) {
              doubles[x] = decompressed.readDouble();
            }
            if (decompressed.readBoolean()) {
              nulls[i] = BitSet.valueOf(decompressed.readVarSizeBytes());
            }
            values[i] = doubles;
            continue;
          case 4:
            final boolean[] booleans = new boolean[count];
            for (int x = 0; x < count; x++) {
              booleans[x] = decompressed.readBoolean();
            }
            if (decompressed.readBoolean()) {
              nulls[i] = BitSet.valueOf(decompressed.readVarSizeBytes());
            }
            values[i] = booleans;
            continue;
          case 5:
            values[i] = decompressed;
            continue;
          case 6:
            final JsonParser hack = factory.createParser(decompressed);
            values[i] = Iterators.get(hack.readValuesAs(Object[].class), 0);
            continue;
          default:
            throw new ISE("invalid type %d", category[i]);
        }
      }
      jp.nextToken();
      return new BulkRow(count, category, values, nulls, encoded);
    }
  };

  private final int count;
  private final int[] category;
  private final Object[] values;
  private final BitSet[] nulls;
  private final int encoded;

  public BulkRow(int count, int[] category, Object[] values, BitSet[] nulls, int encoded)
  {
    this.category = category;
    this.count = count;
    this.values = values;
    this.nulls = nulls;
    this.encoded = encoded;
  }

  public int count()
  {
    return count;
  }

  @VisibleForTesting
  Object[] values()
  {
    return values;
  }

  public Iterator<Object[]> decompose()
  {
    return decompose(false);
  }

  public Iterator<Object[]> decompose(final boolean stringAsRaw)
  {
    IntFunction[] extractors = new IntFunction[values.length];
    for (int i = 0; i < values.length; i++) {
      final int ix = i;
      switch (category[i]) {
        case 1: extractors[i] = x -> ((float[]) values[ix])[x]; continue;
        case 2: extractors[i] = x -> ((long[]) values[ix])[x]; continue;
        case 3: extractors[i] = x -> ((double[]) values[ix])[x]; continue;
        case 4: extractors[i] = x -> ((boolean[]) values[ix])[x]; continue;
        case 5:
          final BytesInputStream source = (BytesInputStream) values[i];
          if (i == encoded) {
            values[i] = FrontCoding.decode(source, count, stringAsRaw ? UTF8Bytes::of : StringUtils::toUTF8String);
          } else {
            final Object[] strings = new Object[count];
            for (int x = 0; x < strings.length; x++) {
              strings[x] = stringAsRaw ? source.viewVarSizeUTF() : source.readVarSizeUTF();
            }
            values[i] = strings;
          }
          // go through
        default:
          extractors[i] = x -> ((Object[]) values[ix])[x];
      }
    }
    return new Iterator<Object[]>()
    {
      private int index;

      @Override
      public boolean hasNext()
      {
        return index < count;
      }

      @Override
      public Object[] next()
      {
        final int ix = index++;
        final Object[] row = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
          if (nulls[i] == null || !nulls[i].get(ix)) {
            row[i] = extractors[i].apply(ix);
          }
        }
        return row;
      }
    };
  }

  @Override
  public Object getRaw(String dimension)
  {
    throw new UnsupportedOperationException("getRaw");
  }

  @Override
  public Collection<String> getColumns()
  {
    throw new UnsupportedOperationException("getColumns");
  }
}
