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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("sketch")
public class GenericSketchAggregatorFactory extends AggregatorFactory.TypeResolving
    implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 24;

  private final String name;
  private final String fieldName;

  private final int sketchParam;
  private final SketchOp sketchOp;
  private final ValueDesc inputType;
  private final boolean merge;

  private final List<OrderingSpec> orderingSpecs;

  private transient Comparator sourceComparator;

  @JsonCreator
  public GenericSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("inputType") ValueDesc inputType,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("orderingSpecs") List<OrderingSpec> orderingSpecs,
      @JsonProperty("merge") boolean merge
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = fieldName == null ? name : fieldName;
    this.inputType = inputType;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? this.sketchOp.defaultParam() : this.sketchOp.normalize(sketchParam);
    this.orderingSpecs = orderingSpecs;
    this.merge = merge;
  }

  private Comparator sourceComparator()
  {
    if (sourceComparator == null) {
      sourceComparator = StringComparators.makeComparator(inputType, orderingSpecs);
    }
    return sourceComparator;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    if (merge) {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      if (selector == null) {
        return Aggregator.NULL;
      }
      return new BaseAggregator(inputType)
      {
        @Override
        public TypedSketch aggregate(TypedSketch current)
        {
          TypedSketch sketch = (TypedSketch) selector.get();
          if (sketch != null) {
            return updateWithSketch(current, sketch.value());
          }
          return current;
        }
      };
    }

    ValueDesc columnType = metricFactory.resolve(fieldName);
    if (columnType == null) {
      return Aggregator.NULL;
    }
    if (ValueDesc.isDimension(columnType)) {
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return new BaseAggregator(selector.type())
      {
        @Override
        public TypedSketch aggregate(TypedSketch current)
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 1) {
            return updateWithValue(current, selector.lookupName(row.get(0)));
          } else if (size > 1) {
            for (int i = 0; i < size; i++) {
              current = updateWithValue(current, selector.lookupName(row.get(i)));
            }
          }
          return current;
        }
      };
    }
    if (ValueDesc.isMultiValued(columnType)) {
      ValueDesc elementType = columnType.subElement();
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      return new BaseAggregator(elementType)
      {
        @Override
        public TypedSketch aggregate(TypedSketch current)
        {
          Object value = selector.get();
          if (value == null) {
            return current;
          }
          if (value.getClass().isArray()) {
            int size = Array.getLength(value);
            for (int i = 0; i < size; i++) {
              current = updateWithValue(current, Array.get(value, i));
            }
          } else {
            current = updateWithValue(current, value);
          }
          return current;
        }
      };
    }
    return new BaseAggregator(columnType)
    {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

      @Override
      public TypedSketch aggregate(TypedSketch current)
      {
        return updateWithValue(current, selector.get());
      }
    };
  }

  @Override
  public boolean needResolving()
  {
    return inputType == null;
  }

  @Override
  public AggregatorFactory resolve(Supplier<? extends TypeResolver> resolver)
  {
    ValueDesc inputType = resolver.get().resolve(fieldName);
    if (inputType.isDimension()) {
      inputType = ValueDesc.STRING;
    }
    return new GenericSketchAggregatorFactory(name, fieldName, inputType, sketchOp, sketchParam, orderingSpecs, merge);
  }

  @SuppressWarnings("unchecked")
  abstract class BaseAggregator implements Aggregator<TypedSketch>
  {
    private final SketchHandler<?> handler = new SketchHandler.Synchronized<>(sketchOp.handler());

    protected BaseAggregator(ValueDesc type)
    {
      if (!handler.supports(type)) {
        throw new UnsupportedOperationException("not supported type " + type);
      }
      if (!ValueDesc.isSameCategory(inputType, type)) {
        throw new UnsupportedOperationException("type mismatch.. " + inputType + " with real type " + type);
      }
    }

    final TypedSketch updateWithValue(TypedSketch sketch, Object value)
    {
      if (sketch == null) {
        sketch = newSketch();
      }
      handler.updateWithValue(sketch, value);
      return sketch;
    }

    final TypedSketch updateWithSketch(TypedSketch sketch, Object value)
    {
      if (sketch == null) {
        sketch = newSketch();
      }
      handler.updateWithSketch(sketch, value);
      return sketch;
    }

    @Override
    public Object get(TypedSketch current)
    {
      return handler.toSketch(current);
    }

    private TypedSketch newSketch()
    {
      return handler.newUnion(sketchParam, inputType, sourceComparator());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    if (merge) {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      if (selector == null) {
        return BufferAggregator.NULL;
      }
      return new BufferAggregator()
      {
        private final SketchHandler<?> handler = new SketchHandler.Synchronized<>(sketchOp.handler());
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
              sketches.set(index, union = handler.newUnion(sketchParam, inputType, sourceComparator()));
            }
            handler.updateWithSketch(union, sketch.value());
          }
        }

        @Override
        public Object get(ByteBuffer buf, int position)
        {
          final TypedSketch sketch = sketches.get(buf.getInt(position));
          return sketch == null ? null : handler.toSketch(sketch);
        }
      };
    }

    final ValueDesc columnType = metricFactory.resolve(fieldName);
    if (columnType == null) {
      return BufferAggregator.NULL;
    }
    if (ValueDesc.isDimension(columnType)) {
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return new BaseBufferAggregator(selector.type())
      {
        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 1) {
            updateWithValue(buf, position, selector.lookupName(row.get(0)));
          } else if (size > 1) {
            for (int i = 0; i < size; i++) {
              updateWithValue(buf, position, selector.lookupName(row.get(i)));
            }
          }
        }
      };
    }
    if (ValueDesc.isMultiValued(columnType)) {
      ValueDesc elementType = columnType.subElement();
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      return new BaseBufferAggregator(elementType)
      {
        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          Object value = selector.get();
          if (value == null) {
            return;
          }
          if (value.getClass().isArray()) {
            int size = Array.getLength(value);
            for (int i = 0; i < size; i++) {
              updateWithValue(buf, position, Array.get(value, i));
            }
          } else {
            updateWithValue(buf, position, value);
          }
        }
      };
    }
    return new BaseBufferAggregator(columnType)
    {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        updateWithValue(buf, position, selector.get());
      }
    };
  }

  @SuppressWarnings("unchecked")
  private abstract class BaseBufferAggregator implements BufferAggregator
  {
    private final SketchHandler<?> handler = sketchOp.handler();
    private final List<TypedSketch> sketches = Lists.newArrayList();

    protected BaseBufferAggregator(ValueDesc type)
    {
      if (!handler.supports(type)) {
        throw new UnsupportedOperationException("not supported type " + type);
      }
      if (!ValueDesc.isSameCategory(inputType, type)) {
        throw new UnsupportedOperationException("type mismatch.. " + inputType + " with real type " + type);
      }
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      buf.putInt(position, sketches.size());
      sketches.add(handler.newUnion(sketchParam, inputType, sourceComparator()));
    }

    final void updateWithValue(ByteBuffer buf, int position, Object value)
    {
      handler.updateWithValue(sketches.get(buf.getInt(position)), value);
    }

    final void updateWithSketch(ByteBuffer buf, int position, Object value)
    {
      handler.updateWithSketch(sketches.get(buf.getInt(position)), value);
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return handler.toSketch(sketches.get(buf.getInt(position)));
    }
  }

  @Override
  public Object deserialize(Object object)
  {
    return TypedSketch.deserialize(sketchOp, object, sourceComparator());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object finalizeComputation(Object object)
  {
    return object == null || sketchOp != SketchOp.THETA ? object : ((TypedSketch<Sketch>) object).value().getEstimate();
  }

  @Override
  public ValueDesc finalizedType()
  {
    return sketchOp == SketchOp.THETA ? ValueDesc.DOUBLE : getOutputType();
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
  public Combiner<TypedSketch> combiner()
  {
    return new Combiner<TypedSketch>()
    {
      @Override
      public TypedSketch combine(TypedSketch param1, TypedSketch param2)
      {
        Preconditions.checkArgument(
            param1.type().equals(param2.type()),
            "Type mismatch.. %s with %s", param1.type(), param2.type()
        );
        // hack to get consistent sketch from cached segment
        ItemsSketch.rand.setSeed(0);
//    ItemsSketch.rand.get().setSeed(0);    // pending PR (https://github.com/DataSketches/sketches-core/pull/190)
        SketchHandler<?> handler = sketchOp.handler();
        TypedSketch union = handler.newUnion(sketchParam, param1.type(), sourceComparator());
        handler.updateWithSketch(union, param1.value());
        handler.updateWithSketch(union, param2.value());
        return handler.toSketch(union);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GenericSketchAggregatorFactory(name, name, inputType, sketchOp, sketchParam, orderingSpecs, true);
  }

  @Override
  @JsonProperty
  public ValueDesc getInputType()
  {
    return inputType;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public String getCubeName()
  {
    return "sketch";
  }

  @Override
  public String getPredicate()
  {
    return null;
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new GenericSketchAggregatorFactory(inputField, name, inputType, sketchOp, sketchParam, orderingSpecs, true);
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
  public List<OrderingSpec> getOrderingSpecs()
  {
    return orderingSpecs;
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
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName)
                  .append(inputType)
                  .append(sketchOp)
                  .append(sketchParam)
                  .append(orderingSpecs)
                  .append(merge);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("sketch." + sketchOp);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name='" + name + '\''
           + ", fieldName='" + fieldName + '\''
           + ", inputType=" + inputType
           + ", sketchOp=" + sketchOp
           + ", sketchParam=" + sketchParam
           + ", orderingSpecs=" + orderingSpecs
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
    if (!Objects.equals(inputType, that.inputType)) {
      return false;
    }
    if (sketchOp != that.sketchOp) {
      return false;
    }
    if (sketchParam != that.sketchParam) {
      return false;
    }
    if (!Objects.equals(orderingSpecs, that.orderingSpecs)) {
      return false;
    }
    return merge == that.merge;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + Objects.hashCode(inputType);
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + sketchParam;
    result = 31 * result + Objects.hashCode(orderingSpecs);
    result = 31 * result + (merge ? 1 : 0);
    return result;
  }
}
