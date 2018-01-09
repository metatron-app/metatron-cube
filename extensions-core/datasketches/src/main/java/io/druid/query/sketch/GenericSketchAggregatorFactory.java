/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("sketch")
public class GenericSketchAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 24;

  private final String name;
  private final String fieldName;

  private final int sketchParam;
  private final SketchOp sketchOp;
  private final String stringComparator;
  private final boolean merge;

  @JsonCreator
  public GenericSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("stringComparator") String stringComparator,
      @JsonProperty("merge") boolean merge
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = fieldName == null ? name : fieldName;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? this.sketchOp.defaultParam() : this.sketchOp.normalize(sketchParam);
    this.stringComparator = StringComparators.validate(stringComparator);
    this.merge = merge;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ValueDesc type = metricFactory.getColumnType(fieldName);
    if (type == null) {
      return new Aggregator.Null();
    }
    final Comparator comparator = stringComparator != null ? StringComparators.makeComparator(stringComparator) : null;
    final SketchHandler<?> handler = new SketchHandler.Synchronized<>(sketchOp.handler());
    if (ValueDesc.isDimension(type)) {
      Preconditions.checkArgument(!merge, "invalid state");
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return new Aggregator.Abstract()
      {
        final TypedSketch sketch = handler.newUnion(sketchParam, ValueType.STRING, comparator);

        @Override
        public void aggregate()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 1) {
            handler.updateWithValue(sketch, Strings.nullToEmpty(selector.lookupName(row.get(0))));
          } else if (size > 1) {
            for (int i = 0; i < size; i++) {
              handler.updateWithValue(sketch, Strings.nullToEmpty(selector.lookupName(row.get(i))));
            }
          }
        }

        @Override
        public void reset()
        {
          handler.reset(sketch);
        }

        @Override
        public Object get()
        {
          return handler.toSketch(sketch);
        }
      };
    }
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (merge) {
      Preconditions.checkArgument(type.equals(ValueDesc.of(getTypeName())));
      return new Aggregator.Abstract()
      {
        TypedSketch union;

        @Override
        public void aggregate()
        {
          final TypedSketch sketch = (TypedSketch) selector.get();
          if (sketch != null) {
            if (union == null) {
              union = handler.newUnion(sketchParam, sketch.type(), comparator);
            }
            handler.updateWithSketch(union, sketch.value());
          }
        }

        @Override
        public void reset()
        {
          handler.reset(union);
        }

        @Override
        public Object get()
        {
          return union == null ? null : handler.toSketch(union);
        }
      };
    }
    if (!handler.supports(type.type())) {
      throw new UnsupportedOperationException("not supported type " + type);
    }
    return new Aggregator.Abstract()
    {
      final TypedSketch sketch = handler.newUnion(sketchParam, type.type(), comparator);

      @Override
      public void aggregate()
      {
        handler.updateWithValue(sketch, selector.get());
      }

      @Override
      public void reset()
      {
        handler.reset(sketch);
      }

      @Override
      public Object get()
      {
        return handler.toSketch(sketch);
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ValueDesc type = metricFactory.getColumnType(fieldName);
    if (type == null) {
      return new BufferAggregator.Null();
    }
    final Comparator comparator = stringComparator != null ? StringComparators.makeComparator(stringComparator) : null;
    final SketchHandler<?> handler = sketchOp.handler();
    if (ValueDesc.isDimension(type)) {
      Preconditions.checkArgument(!merge, "invalid state");
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return new BufferAggregator.Abstract()
      {
        private final List<TypedSketch> sketches = Lists.newArrayList();

        @Override
        public void init(ByteBuffer buf, int position)
        {
          buf.putInt(position, sketches.size());
          sketches.add(handler.newUnion(sketchParam, ValueType.STRING, comparator));
        }

        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 1) {
            final TypedSketch sketch = sketches.get(buf.getInt(position));
            handler.updateWithValue(sketch, Strings.nullToEmpty(selector.lookupName(row.get(0))));
          } else if (size > 1) {
            final TypedSketch sketch = sketches.get(buf.getInt(position));
            for (int i = 0; i < size; i++) {
              handler.updateWithValue(sketch, Strings.nullToEmpty(selector.lookupName(row.get(i))));
            }
          }
        }

        @Override
        public Object get(ByteBuffer buf, int position)
        {
          return handler.toSketch(sketches.get(buf.getInt(position)));
        }
      };
    }
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (merge) {
      Preconditions.checkArgument(type.equals(ValueDesc.of(getTypeName())));
      return new BufferAggregator.Abstract()
      {
        private final List<TypedSketch> sketches = Lists.newArrayList();

        @Override
        public void init(ByteBuffer buf, int position)
        {
          buf.putInt(position, sketches.size());
          sketches.add(null);
        }

        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final TypedSketch sketch = (TypedSketch) selector.get();
          if (sketch != null) {
            final int index = buf.getInt(position);
            TypedSketch union = sketches.get(index);
            if (union == null) {
              sketches.set(index, union = handler.newUnion(sketchParam, sketch.type(), comparator));
            }
            handler.updateWithSketch(union, sketch.value());
          }
        }

        @Override
        public Object get(ByteBuffer buf, int position)
        {
          TypedSketch sketch = sketches.get(buf.getInt(position));
          return sketch == null ? null : handler.toSketch(sketch);
        }
      };
    }
    if (!handler.supports(type.type())) {
      throw new UnsupportedOperationException("not supported type " + type);
    }
    return new BufferAggregator.Abstract()
    {
      private final List<TypedSketch> sketches = Lists.newArrayList();

      @Override
      public void init(ByteBuffer buf, int position)
      {
        buf.putInt(position, sketches.size());
        sketches.add(handler.newUnion(sketchParam, type.type(), comparator));
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        handler.updateWithValue(sketches.get(buf.getInt(position)), selector.get());
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        return handler.toSketch(sketches.get(buf.getInt(position)));
      }
    };
  }

  @Override
  public Object deserialize(Object object)
  {
    return TypedSketch.deserialize(sketchOp, object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object finalizeComputation(Object object)
  {
    return sketchOp == SketchOp.THETA ? ((TypedSketch<Sketch>) object).value().getEstimate() : object;
  }

  @Override
  public Comparator getComparator()
  {
    if (sketchOp == SketchOp.THETA) {
      return Ordering.natural();
    }
    throw new UnsupportedOperationException("getComparator");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object combine(Object lhs, Object rhs)
  {
    TypedSketch object1 = (TypedSketch) lhs;
    TypedSketch object2 = (TypedSketch) rhs;
    Preconditions.checkArgument(
        object1.type() == object2.type(),
        "Type mismatch.. " + object1.type() + " with " + object2.type()
    );
    SketchHandler<?> handler = sketchOp.handler();
    Comparator comparator = stringComparator != null ? StringComparators.makeComparator(stringComparator) : null;
    TypedSketch union = handler.newUnion(sketchParam, object1.type(), comparator);
    handler.updateWithSketch(union, object1.value());
    handler.updateWithSketch(union, object2.value());
    return handler.toSketch(union);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GenericSketchAggregatorFactory(name, name, sketchOp, sketchParam, stringComparator, true);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public SketchOp getSketchOp()
  {
    return sketchOp;
  }

  @JsonProperty
  public int getSketchParam()
  {
    return sketchParam;
  }

  @JsonProperty
  public String getStringComparator()
  {
    return stringComparator;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Ints.BYTES;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] comparatorBytes = StringUtils.toUtf8WithNullToEmpty(stringComparator);
    int length = 7 + fieldNameBytes.length + comparatorBytes.length;

    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put((byte) sketchOp.ordinal())
                     .putInt(sketchParam)
                     .put(comparatorBytes)
                     .put((byte) (merge ? 1 : 0))
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "sketch." + sketchOp;
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name='" + name + '\''
           + ", fieldName='" + fieldName + '\''
           + ", sketchOp=" + sketchOp
           + ", sketchParam=" + sketchParam
           + ", stringComparator=" + stringComparator
           + ", merge=" + merge
           + '}';
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericSketchAggregatorFactory that = (GenericSketchAggregatorFactory) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (sketchOp != that.sketchOp) {
      return false;
    }
    if (sketchParam != that.sketchParam) {
      return false;
    }
    if (!Objects.equals(stringComparator, that.stringComparator)) {
      return false;
    }
    return merge == that.merge;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + sketchParam;
    result = 31 * result + Objects.hashCode(stringComparator);
    result = 31 * result + (merge ? 1 : 0);
    return result;
  }
}
