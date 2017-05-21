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
import com.metamx.common.Pair;
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
import io.druid.data.ValueType;

/**
 */
public abstract class TypedSketch<T> extends Pair<ValueType, T>
{
  public static TypedSketch of(final ValueType type, final Object value)
  {
    if (value instanceof Union) {
      return of(type, (Union)value);
    }
    if (value instanceof Sketch) {
      return of(type, (Sketch)value);
    }
    if (value instanceof ItemsUnion) {
      return of(type, (ItemsUnion)value);
    }
    if (value instanceof ItemsSketch) {
      return of(type, (ItemsSketch)value);
    }
    if (value instanceof com.yahoo.sketches.frequencies.ItemsSketch) {
      return of(type, (com.yahoo.sketches.frequencies.ItemsSketch)value);
    }
    if (value instanceof ReservoirItemsUnion) {
      return of(type, (ReservoirItemsUnion)value);
    }
    if (value instanceof ReservoirItemsSketch) {
      return of(type, (ReservoirItemsSketch)value);
    }
    throw new IllegalArgumentException("Not supported type.. " + value);
  }

  public static TypedSketch<Union> of(final ValueType type, final Union sketch)
  {
    return new TypedSketch<Union>(type, sketch) {

      @Override
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray();
      }
    };
  }

  public static TypedSketch<Sketch> of(final ValueType type, final Sketch sketch)
  {
    return new TypedSketch<Sketch>(type, sketch) {

      @Override
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray();
      }
    };
  }

  public static TypedSketch<ItemsUnion> of(final ValueType type, final ItemsUnion sketch)
  {
    return new TypedSketch<ItemsUnion>(type, sketch) {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.getResult().toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ItemsSketch> of(final ValueType type, final ItemsSketch sketch)
  {
    return new TypedSketch<ItemsSketch>(type, sketch) {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch> of(final ValueType type, final com.yahoo.sketches.frequencies.ItemsSketch sketch)
  {
    return new TypedSketch<com.yahoo.sketches.frequencies.ItemsSketch>(type, sketch) {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ReservoirItemsUnion> of(final ValueType type, final ReservoirItemsUnion sketch)
  {
    return new TypedSketch<ReservoirItemsUnion>(type, sketch) {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  public static TypedSketch<ReservoirItemsSketch> of(final ValueType type, final ReservoirItemsSketch sketch)
  {
    return new TypedSketch<ReservoirItemsSketch>(type, sketch) {

      @Override
      @SuppressWarnings("unchecked")
      public byte[] sketchToBytes()
      {
        return sketch.toByteArray(toItemsSerDe(type));
      }
    };
  }

  @JsonCreator
  public TypedSketch(@JsonProperty ValueType lhs, @JsonProperty T rhs)
  {
    super(lhs, rhs);
  }

  @JsonProperty
  public ValueType type() { return lhs; }

  @JsonProperty
  public T value() { return rhs; }

  @JsonValue
  public byte[] toByteArray()
  {
    byte[] sketch = sketchToBytes();
    byte[] typed = new byte[sketch.length + 1];
    System.arraycopy(sketch, 0, typed, 1, sketch.length);
    typed[0] = (byte) lhs.ordinal();
    return typed;
  }

  public abstract byte[] sketchToBytes();

  private static final ArrayOfItemsSerDe<Float> floatsSerDe = new ArrayOfFloatsSerDe();
  private static final ArrayOfItemsSerDe<Double> doublesSerDe = new ArrayOfDoublesSerDe();
  private static final ArrayOfItemsSerDe<Long> longsSerDe = new ArrayOfLongsSerDe();
  private static final ArrayOfItemsSerDe<String> stringsSerDe = new ArrayOfStringsSerDe();


  public static ArrayOfItemsSerDe toItemsSerDe(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return floatsSerDe;
      case DOUBLE:
        return doublesSerDe;
      case LONG:
        return longsSerDe;
      case STRING:
        return stringsSerDe;
      default:
        throw new UnsupportedOperationException("unsupported type " + type);
    }
  }
}
