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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;

public class BulkRow extends AbstractRow
{
  private static final LZ4FastDecompressor LZ4 = LZ4Factory.fastestInstance().fastDecompressor();

  public static final TypeReference<BulkRow> TYPE_REFERENCE = new TypeReference<BulkRow>() {};

  public static final JsonSerializer<BulkRow> SERIALIZER = new JsonSerializer<BulkRow>()
  {
    @Override
    public void serializeWithType(BulkRow bulk, JsonGenerator jgen, SerializerProvider provider, TypeSerializer typeSer)
        throws IOException
    {
      typeSer.writeTypePrefixForObject(bulk, jgen);   // see mixin in AggregatorsModule
      jgen.writeFieldName("v");
      serialize(bulk, jgen, provider);
      typeSer.writeTypeSuffixForObject(bulk, jgen);
    }

    @Override
    public void serialize(BulkRow bulk, JsonGenerator jgen, SerializerProvider provider) throws IOException
    {
      final BitSet nulls = new BitSet();
      final BytesOutputStream output = new BytesOutputStream();
      final ObjectMapper mapper = (ObjectMapper) Preconditions.checkNotNull(jgen.getCodec());
      final JsonGenerator hack = mapper.getFactory().createGenerator(output);

      output.writeUnsignedVarInt(bulk.count);
      output.writeUnsignedVarInt(bulk.category.length);

      for (int i = 0; i < bulk.category.length; i++) {
        nulls.clear();
        output.writeUnsignedVarInt(bulk.category[i]);
        switch (bulk.category[i]) {
          case 0:
            output.writeVarSizeBytes((byte[]) bulk.values[i]);
            continue;
          case 1:
            final Float[] floats = (Float[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              if (floats[x] == null) {
                output.writeFloat(0);
                nulls.set(x);
              } else {
                output.writeFloat(floats[x].floatValue());
              }
            }
            if (!nulls.isEmpty()) {
              output.writeBoolean(true);
              output.writeVarSizeBytes(nulls.toByteArray());
            } else {
              output.writeBoolean(false);
            }
            continue;
          case 2:
            final Long[] longs = (Long[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              if (longs[x] == null) {
                output.writeLong(0);
                nulls.set(x);
              } else {
                output.writeLong(longs[x].longValue());
              }
            }
            if (!nulls.isEmpty()) {
              output.writeBoolean(true);
              output.writeVarSizeBytes(nulls.toByteArray());
            } else {
              output.writeBoolean(false);
            }
            continue;
          case 3:
            final Double[] doubles = (Double[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              if (doubles[x] == null) {
                output.writeDouble(0);
                nulls.set(x);
              } else {
                output.writeDouble(doubles[x].doubleValue());
              }
            }
            if (!nulls.isEmpty()) {
              output.writeBoolean(true);
              output.writeVarSizeBytes(nulls.toByteArray());
            } else {
              output.writeBoolean(false);
            }
            continue;
          case 4:
            final Boolean[] booleans = (Boolean[]) bulk.values[i];
            for (int x = 0; x < bulk.count; x++) {
              if (booleans[x] == null) {
                output.writeBoolean(false);
                nulls.set(x);
              } else {
                output.writeBoolean(booleans[x].booleanValue());
              }
            }
            if (!nulls.isEmpty()) {
              output.writeBoolean(true);
              output.writeVarSizeBytes(nulls.toByteArray());
            } else {
              output.writeBoolean(false);
            }
            continue;
          case 5:
            output.writeVarSizeBytes((BytesRef) bulk.values[i]);
            continue;
          case 6:
            hack.writeObject(bulk.values[i]);
            hack.flush();
            continue;
          default:
            throw new ISE("invalid type %d", bulk.category[i]);
        }
      }
      BytesRef ref = output.asRef();
      jgen.writeBinary(ref.bytes, 0, ref.length);
    }
  };

  // todo : keep primitive array and null bitset
  public static final JsonDeserializer<BulkRow> DESERIALIZER = new JsonDeserializer<BulkRow>()
  {
    @Override
    public BulkRow deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
    {
      Preconditions.checkArgument(jp.getCurrentToken() == JsonToken.FIELD_NAME);
      jp.nextToken();
      final BytesInputStream input = new BytesInputStream(jp.getBinaryValue());
      final ObjectMapper mapper = (ObjectMapper) Preconditions.checkNotNull(jp.getCodec());
      final JsonParser hack = mapper.getFactory().createParser(input);

      final int count = input.readUnsignedVarInt();
      final int[] category = new int[input.readUnsignedVarInt()];
      final Object[] values = new Object[category.length];
      for (int i = 0; i < category.length; i++) {
        switch (category[i] = input.readUnsignedVarInt()) {
          case 0:
            values[i] = new TimestampRLE(input.readVarSizeBytes()).iterator();
            continue;
          case 1:
            final Float[] floats = new Float[count];
            for (int x = 0; x < count; x++) {
              floats[x] = input.readFloat();
            }
            if (input.readBoolean()) {
              final BitSet bitSet = BitSet.valueOf(input.readVarSizeBytes());
              for (int index = bitSet.nextSetBit(0); index >= 0; index = bitSet.nextSetBit(index + 1)) {
                floats[index] = null;
              }
            }
            values[i] = floats;
            continue;
          case 2:
            final Long[] longs = new Long[count];
            for (int x = 0; x < count; x++) {
              longs[x] = input.readLong();
            }
            if (input.readBoolean()) {
              final BitSet bitSet = BitSet.valueOf(input.readVarSizeBytes());
              for (int index = bitSet.nextSetBit(0); index >= 0; index = bitSet.nextSetBit(index + 1)) {
                longs[index] = null;
              }
            }
            values[i] = longs;
            continue;
          case 3:
            final Double[] doubles = new Double[count];
            for (int x = 0; x < count; x++) {
              doubles[x] = input.readDouble();
            }
            if (input.readBoolean()) {
              final BitSet bitSet = BitSet.valueOf(input.readVarSizeBytes());
              for (int index = bitSet.nextSetBit(0); index >= 0; index = bitSet.nextSetBit(index + 1)) {
                doubles[index] = null;
              }
            }
            values[i] = doubles;
            continue;
          case 4:
            final Boolean[] booleans = new Boolean[count];
            for (int x = 0; x < count; x++) {
              booleans[x] = input.readBoolean();
            }
            if (input.readBoolean()) {
              final BitSet bitSet = BitSet.valueOf(input.readVarSizeBytes());
              for (int index = bitSet.nextSetBit(0); index >= 0; index = bitSet.nextSetBit(index + 1)) {
                booleans[index] = null;
              }
            }
            values[i] = booleans;
            continue;
          case 5:
            final byte[] array = input.readVarSizeBytes();
            values[i] = new BytesInputStream(LZ4.decompress(array, Ints.BYTES, Ints.fromByteArray(array)));
            continue;
          case 6:
            values[i] = Iterators.getOnlyElement(hack.readValuesAs(Object[].class));
            continue;
          default:
            throw new ISE("invalid type %d", category[i]);
        }
      }
      jp.nextToken();
      return new BulkRow(count, category, values);
    }
  };

  private final int count;
  private final int[] category;
  private final Object[] values;

  public BulkRow(int count, int[] category, Object[] values)
  {
    this.category = category;
    this.count = count;
    this.values = values;
  }

  public int count()
  {
    return count;
  }

  public Object[] values()
  {
    return values;
  }

  protected BulkRow forTest()
  {
    for (int i = 0; i < category.length; i++) {
      if (category[i] == 0 && values[i] instanceof byte[]) {
        values[i] = new TimestampRLE((byte[]) values[i]).iterator();
      } else if (category[i] == 5 && values[i] instanceof BytesRef) {
        final BytesRef array = (BytesRef) values[i];
        values[i] = new BytesInputStream(LZ4.decompress(array.bytes, Ints.BYTES, Ints.fromByteArray(array.bytes)));
      }
    }
    return this;
  }

  public Sequence<Object[]> decompose()
  {
    return Sequences.once(
        new Iterator<Object[]>()
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
            for (int i = 0; i < row.length; i++) {
              if (values[i] instanceof Iterator) {
                Iterator iterator = (Iterator) values[i];
                row[i] = iterator.hasNext() ? iterator.next() : -1;   // todo: increases on hasNext()
              } else if (values[i] instanceof BytesInputStream) {
                row[i] = ((BytesInputStream) values[i]).readVarSizeUTF();
              } else {
                row[i] = Array.get(values[i], ix);
              }
            }
            return row;
          }
        }
    );
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
