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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.UTF8Bytes;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.ordering.OrderingSpec;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelector.SingleValued;
import io.druid.segment.DimensionSelector.WithRawAccess;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.IndexedInts;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

  private final String predicate;
  private final ValueDesc inputType;

  private final int sketchParam;
  private final SketchOp sketchOp;
  private final boolean merge;

  private final OrderingSpec orderingSpec;

  private transient Comparator sourceComparator;

  @JsonCreator
  public GenericSketchAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("inputType") ValueDesc inputType,
      @JsonProperty("sketchOp") SketchOp sketchOp,
      @JsonProperty("sketchParam") Integer sketchParam,
      @JsonProperty("orderingSpec") OrderingSpec orderingSpec,
      @JsonProperty("merge") boolean merge
  )
  {
    this.name = Preconditions.checkNotNull(name, "'name' cannot be null");
    this.fieldName = fieldName == null ? name : fieldName;
    this.predicate = predicate;
    this.inputType = inputType;
    this.sketchOp = sketchOp == null ? SketchOp.THETA : sketchOp;
    this.sketchParam = sketchParam == null ? this.sketchOp.defaultParam() : this.sketchOp.normalize(sketchParam);
    this.orderingSpec = orderingSpec;
    this.merge = merge;
  }

  private Comparator sourceComparator()
  {
    if (sourceComparator == null) {
      List<OrderingSpec> orderingSpecs = orderingSpec == null ? null : Arrays.asList(orderingSpec);
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

    final ValueDesc columnType = metricFactory.resolve(fieldName);
    if (columnType == null) {
      return Aggregator.NULL;
    }
    final ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, metricFactory);
    if (columnType.isDimension()) {
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      if (selector instanceof SingleValued) {
        if (selector instanceof WithRawAccess) {
          return Aggregators.wrap(matcher, new BaseAggregator(selector.type())
          {
            @Override
            public TypedSketch aggregate(TypedSketch current)
            {
              return updateWithValue(
                  current, UTF8Bytes.of(((WithRawAccess) selector).lookupRaw(selector.getRow().get(0)))
              );
            }
          });
        }
        return Aggregators.wrap(matcher, new BaseAggregator(selector.type())
        {
          @Override
          public TypedSketch aggregate(TypedSketch current)
          {
            return updateWithValue(current, selector.lookupName(selector.getRow().get(0)));
          }
        });
      }
      return Aggregators.wrap(matcher, new BaseAggregator(selector.type())
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
      });
    }
    if (columnType.isMultiValued()) {
      final ValueDesc elementType = columnType.subElement();
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      return Aggregators.wrap(matcher, new BaseAggregator(elementType)
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
      });
    }
    return Aggregators.wrap(matcher, new BaseAggregator(columnType)
    {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

      @Override
      public TypedSketch aggregate(TypedSketch current)
      {
        return updateWithValue(current, selector.get());
      }
    });
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
    return new GenericSketchAggregatorFactory(
        name,
        fieldName,
        predicate,
        inputType,
        sketchOp,
        sketchParam,
        orderingSpec,
        merge
    );
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
      sketch = sketch == null ? newSketch() : sketch;
      handler.updateWithValue(sketch, value);
      return sketch;
    }

    final TypedSketch updateWithSketch(TypedSketch sketch, Object value)
    {
      sketch = sketch == null ? newSketch() : sketch;
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
    final ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, metricFactory);
    if (columnType.isDimension()) {
      final DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return Aggregators.wrap(matcher, new BaseBufferAggregator(selector.type())
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
      });
    }
    if (columnType.isMultiValued()) {
      final ValueDesc elementType = columnType.subElement();
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
      return Aggregators.wrap(matcher, new BaseBufferAggregator(elementType)
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
      });
    }
    return Aggregators.wrap(matcher, new BaseBufferAggregator(columnType)
    {
      final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        updateWithValue(buf, position, selector.get());
      }
    });
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
      return GuavaUtils.nullFirstNatural();
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
    return new GenericSketchAggregatorFactory(
        name,
        name,
        null,
        inputType,
        sketchOp,
        sketchParam,
        orderingSpec,
        true
    );
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
    return String.format("sketch(op=%s|param=%d)", sketchOp.name().toLowerCase(), sketchParam);
  }

  @Override
  @JsonProperty
  public String getPredicate()
  {
    return predicate;
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
    return new GenericSketchAggregatorFactory(
        name,
        inputField,
        null,
        inputType,
        sketchOp,
        sketchParam,
        orderingSpec,
        true
    );
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
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public OrderingSpec getOrderingSpecs()
  {
    return orderingSpec;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Integer.BYTES;
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
                  .append(predicate)
                  .append(inputType)
                  .append(sketchOp)
                  .append(sketchParam)
                  .append(orderingSpec)
                  .append(merge);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return TypedSketch.of(sketchOp);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name='" + name + '\''
           + ", fieldName='" + fieldName + '\''
           + (predicate == null ? "": ", predicate=" + predicate)
           + ", inputType=" + inputType
           + ", sketchOp=" + sketchOp
           + ", sketchParam=" + sketchParam
           + ", orderingSpec=" + orderingSpec
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
    if (!Objects.equals(predicate, that.predicate)) {
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
    if (!Objects.equals(orderingSpec, that.orderingSpec)) {
      return false;
    }
    return merge == that.merge;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + Objects.hashCode(inputType);
    result = 31 * result + sketchOp.ordinal();
    result = 31 * result + sketchParam;
    result = 31 * result + Objects.hashCode(orderingSpec);
    result = 31 * result + (merge ? 1 : 0);
    return result;
  }
}
