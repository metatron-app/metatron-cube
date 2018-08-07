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
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.data.ValueDesc;
import io.druid.query.QueryCacheHelper;
import io.druid.query.RowResolver;
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
{
  private static final byte CACHE_TYPE_ID = 24;

  private final String name;
  private final String fieldName;

  private final int sketchParam;
  private final SketchOp sketchOp;
  private final ValueDesc sourceType;
  private final boolean merge;

  private final List<OrderingSpec> orderingSpecs;

  private transient Comparator sourceComparator;

  @JsonCreator
  public GenericSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("sourceType") ValueDesc sourceType,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("orderingSpecs") List<OrderingSpec> orderingSpecs,
      @JsonProperty("merge") boolean merge
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = fieldName == null ? name : fieldName;
    this.sourceType = sourceType;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? this.sketchOp.defaultParam() : this.sketchOp.normalize(sketchParam);
    this.orderingSpecs = orderingSpecs;
    this.merge = merge;
  }

  private ValueDesc sourceType()
  {
    return sourceType == null ? ValueDesc.STRING : sourceType;
  }

  private Comparator sourceComparator()
  {
    if (sourceComparator == null) {
      sourceComparator = StringComparators.makeComparator(sourceType(), orderingSpecs);
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
      return new BaseAggregator(sourceType())
      {
        @Override
        public void aggregate()
        {
          TypedSketch sketch = (TypedSketch) selector.get();
          if (sketch != null) {
            updateWithSketch(sketch.value());
          }
        }
      };
    }

    ValueDesc columnType = metricFactory.getColumnType(fieldName);
    if (columnType == null) {
      return Aggregator.NULL;
    }
    if (ValueDesc.isDimension(columnType)) {
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return new BaseAggregator(selector.type())
      {
        @Override
        public void aggregate()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 1) {
            updateWithValue(selector.lookupName(row.get(0)));
          } else if (size > 1) {
            for (int i = 0; i < size; i++) {
              updateWithValue(selector.lookupName(row.get(i)));
            }
          }
        }
      };
    }
    if (ValueDesc.isMultiValued(columnType)) {
      ValueDesc elementType = columnType.subElement();
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      return new BaseAggregator(elementType)
      {
        @Override
        public void aggregate()
        {
          Object value = selector.get();
          if (value == null) {
            return;
          }
          if (value.getClass().isArray()) {
            int size = Array.getLength(value);
            for (int i = 0; i < size; i++) {
              updateWithValue(Array.get(value, i));
            }
          } else {
            updateWithValue(value);
          }
        }
      };
    }
    return new BaseAggregator(columnType)
    {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

      @Override
      public void aggregate()
      {
        updateWithValue(selector.get());
      }
    };
  }

  @Override
  public boolean needResolving()
  {
    return sourceType == null;
  }

  @Override
  public AggregatorFactory resolve(RowResolver resolver)
  {
    ValueDesc sourceType = resolver.resolve(fieldName);
    if (sourceType.isDimension()) {
      sourceType = ValueDesc.STRING;
    }
    return new GenericSketchAggregatorFactory(name, fieldName, sourceType, sketchOp, sketchParam, orderingSpecs, merge);
  }

  @SuppressWarnings("unchecked")
  abstract class BaseAggregator extends Aggregator.Abstract
  {
    private final SketchHandler<?> handler = new SketchHandler.Synchronized<>(sketchOp.handler());
    private final TypedSketch sketch = handler.newUnion(sketchParam, sourceType(), sourceComparator());

    protected BaseAggregator(ValueDesc type)
    {
      if (!handler.supports(type)) {
        throw new UnsupportedOperationException("not supported type " + type);
      }
      if (!ValueDesc.isSameCategory(sourceType(), type)) {
        throw new UnsupportedOperationException("type mismatch.. " + sourceType() + " with real type " + type);
      }
    }

    final void updateWithValue(Object value)
    {
      handler.updateWithValue(sketch, value);
    }

    final void updateWithSketch(Object value)
    {
      handler.updateWithSketch(sketch, value);
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
      return new BufferAggregator.Abstract()
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
              sketches.set(index, union = handler.newUnion(sketchParam, sourceType(), sourceComparator()));
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

    ValueDesc columnType = metricFactory.getColumnType(fieldName);
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
  private abstract class BaseBufferAggregator extends BufferAggregator.Abstract
  {
    private final SketchHandler<?> handler = sketchOp.handler();
    private final List<TypedSketch> sketches = Lists.newArrayList();

    protected BaseBufferAggregator(ValueDesc type)
    {
      if (!handler.supports(type)) {
        throw new UnsupportedOperationException("not supported type " + type);
      }
      if (!ValueDesc.isSameCategory(sourceType(), type)) {
        throw new UnsupportedOperationException("type mismatch.. " + sourceType() + " with real type " + type);
      }
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      buf.putInt(position, sketches.size());
      sketches.add(handler.newUnion(sketchParam, sourceType(), sourceComparator()));
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
        object1.type().equals(object2.type()),
        "Type mismatch.. " + object1.type() + " with " + object2.type()
    );
    // hack to get consistent sketch from cached segment
    ItemsSketch.rand.setSeed(0);
//    ItemsSketch.rand.get().setSeed(0);    // pending PR (https://github.com/DataSketches/sketches-core/pull/190)
    SketchHandler<?> handler = sketchOp.handler();
    TypedSketch union = handler.newUnion(sketchParam, object1.type(), sourceComparator());
    handler.updateWithSketch(union, object1.value());
    handler.updateWithSketch(union, object2.value());
    return handler.toSketch(union);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GenericSketchAggregatorFactory(name, name, sourceType, sketchOp, sketchParam, orderingSpecs, true);
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
  public ValueDesc getSourceType()
  {
    return sourceType;
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
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = QueryCacheHelper.computeCacheBytes(fieldName);
    byte[] sourceTypeBytes = QueryCacheHelper.computeCacheBytes(sourceType == null  ? null : sourceType.typeName());
    byte[] orderingSpecsBytes = QueryCacheHelper.computeAggregatorBytes(orderingSpecs);
    int length = 7 + fieldNameBytes.length + sourceTypeBytes.length + orderingSpecsBytes.length;

    return ByteBuffer.allocate(length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(sourceTypeBytes)
                     .put((byte) sketchOp.ordinal())
                     .putInt(sketchParam)
                     .put(orderingSpecsBytes)
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
           + ", sourceType=" + sourceType
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
    if (!Objects.equals(sourceType, that.sourceType)) {
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
    result = 31 * result + Objects.hashCode(sourceType);
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + sketchParam;
    result = 31 * result + Objects.hashCode(orderingSpecs);
    result = 31 * result + (merge ? 1 : 0);
    return result;
  }
}
