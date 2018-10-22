/*
 * Copyright 2011,2012 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.metamx.common.Pair;
import com.metamx.common.StringUtils;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.ArrayOfItemsSerDe;
import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import com.yahoo.sketches.sampling.ReservoirItemsUnion;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public abstract class TypedSketch<T> extends Pair<ValueDesc, T>
{
  public static TypedSketch deserialize(SketchOp sketchOp, Object bytes, Comparator comparator)
  {
    ByteBuffer value = ByteBuffer.wrap(ThetaOperations.asBytes(bytes));
    ValueDesc type = TypedSketch.typeFromBytes(value);
    return TypedSketch.of(type, deserialize(sketchOp, Memory.wrap(value.slice()), type, comparator));
  }

  private static Object deserialize(
      SketchOp sketchOp,
      Memory memory,
      ValueDesc type,
      Comparator comparator
  )
  {
    switch (sketchOp) {
      case THETA:
        return ThetaOperations.deserializeFromMemory(memory);
      case QUANTILE:
        return ThetaOperations.deserializeQuantileFromMemory(memory, type, comparator);
      case FREQUENCY:
        return ThetaOperations.deserializeFrequencyFromMemory(memory, type);
      case SAMPLING:
        return ThetaOperations.deserializeSamplingFromMemory(memory, type);
    }
    throw new UnsupportedOperationException("invalid type " + sketchOp);
  }

  public static TypedSketch of(final ValueDesc type, final Object value)
  {
    if (value instanceof Union) {
      return of(type, (Union) value);
    }
    if (value instanceof Sketch) {
      return of(type, (Sketch) value);
    }
    if (value instanceof ItemsUnion) {
      return of(type, (ItemsUnion) value);
    }
    if (value instanceof ItemsSketch) {
      return of(type, (ItemsSketch) value);
    }
    if (value instanceof com.yahoo.sketches.frequencies.ItemsSketch) {
      return of(type, (com.yahoo.sketches.frequencies.ItemsSketch) value);
    }
    if (value instanceof ReservoirItemsUnion) {
      return of(type, (ReservoirItemsUnion) value);
    }
    if (value instanceof ReservoirItemsSketch) {
      return of(type, (ReservoirItemsSketch) value);
    }
    throw new IllegalArgumentException("Not supported type.. " + value);
  }

  public static TypedSketch<Union> of(final ValueDesc type, final Union sketch)
  {
    return new TypedSketch<Union>(type, sketch)
    {

      @Override
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray();
      }
    };
  }

  public static TypedSketch<Sketch> of(final ValueDesc type, final Sketch sketch)
  {
    return new TypedSketch<Sketch>(type, sketch)
    {

      @Override
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray();
      }
    };
  }

  public static TypedSketch<ItemsUnion> of(final ValueDesc type, final ItemsUnion sketch)
  {
    return new TypedSketch<ItemsUnion>(type, sketch)
    {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.getResult().toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ItemsSketch> of(final ValueDesc type, final ItemsSketch sketch)
  {
    return new TypedSketch<ItemsSketch>(type, sketch)
    {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> of(
      final ValueDesc type, final com.yahoo.sketches.frequencies.ItemsSketch sketch
  )
  {
    return new TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch>(type, sketch)
    {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ReservoirItemsUnion> of(final ValueDesc type, final ReservoirItemsUnion sketch)
  {
    return new TypedSketch<ReservoirItemsUnion>(type, sketch)
    {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ReservoirItemsSketch> of(final ValueDesc type, final ReservoirItemsSketch sketch)
  {
    return new TypedSketch<ReservoirItemsSketch>(type, sketch)
    {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  @JsonCreator
  public TypedSketch(@JsonProperty ValueDesc lhs, @JsonProperty T rhs)
  {
    super(lhs, rhs);
  }

  @JsonProperty
  public ValueDesc type() { return lhs; }

  @JsonProperty
  public T value() { return rhs; }

  @JsonValue
  public byte[] toByteArray()
  {
    return Bytes.concat(toBytes(lhs), sketchToBytes());
  }

  public abstract byte[] sketchToBytes();

  private static final ArrayOfItemsSerDe<Float> floatsSerDe = new ArrayOfFloatsSerDe();
  private static final ArrayOfItemsSerDe<Double> doublesSerDe = new ArrayOfDoublesSerDe();
  private static final ArrayOfItemsSerDe<Long> longsSerDe = new ArrayOfLongsSerDe();
  private static final ArrayOfItemsSerDe<String> stringsSerDe = new ArrayOfStringsSerDe();

  public static ArrayOfItemsSerDe toItemsSerDe(ValueDesc type)
  {
    switch (type.type()) {
      case FLOAT:
        return floatsSerDe;
      case DOUBLE:
        return doublesSerDe;
      case LONG:
        return longsSerDe;
      case STRING:
        return stringsSerDe;
      default:
        if (type.isStruct()) {
          String[] split = Preconditions.checkNotNull(
              TypeUtils.splitDescriptiveType(type.typeName()),
              "invalid description " + type
          );
          List<ValueType> fields = Lists.newArrayList();
          for (int i = 1; i < split.length; i++) {
            int index = split[i].indexOf(':');
            if (index >= 0) {
              split[i] = split[1].substring(index);    // named field
            }
            fields.add(ValueType.ofPrimitive(split[i]));
          }
          return new ArrayOfStructSerDe(fields);
        }
        throw new UnsupportedOperationException("unsupported type " + type);
    }
  }

  private static byte[] toBytes(ValueDesc type)
  {
    switch (type.type()) {
      case FLOAT:
        return new byte[]{0x00};
      case DOUBLE:
        return new byte[]{0x01};
      case LONG:
        return new byte[]{0x02};
      case STRING:
        return new byte[]{0x03};
      case COMPLEX:
        byte[] bytes = StringUtils.toUtf8(type.typeName());
        Preconditions.checkArgument(bytes.length < 0xFF);
        return ByteBuffer.allocate(1 + 1 + bytes.length).put((byte) 0x04).put((byte) bytes.length).put(bytes).array();
      default:
        throw new UnsupportedOperationException("unsupported type " + type);
    }
  }

  private static ValueDesc typeFromBytes(ByteBuffer value)
  {
    byte type = value.get();
    switch (type) {
      case 0:
        return ValueDesc.FLOAT;
      case 1:
        return ValueDesc.DOUBLE;
      case 2:
        return ValueDesc.LONG;
      case 3:
        return ValueDesc.STRING;
      case 4:
        byte[] desc = new byte[UnsignedBytes.toInt(value.get())];
        value.get(desc);
        return ValueDesc.of(StringUtils.fromUtf8(desc));
      default:
        throw new UnsupportedOperationException("unsupported type " + type);
    }
  }
}
