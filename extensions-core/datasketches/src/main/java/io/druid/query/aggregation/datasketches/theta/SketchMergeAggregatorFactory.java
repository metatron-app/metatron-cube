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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.sketch.ThetaOperations;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("thetaSketch")
public class SketchMergeAggregatorFactory extends AggregatorFactory
{
  public static final int DEFAULT_MAX_SKETCH_SIZE = 16384;

  public static final Comparator<Sketch> COMPARATOR = new Comparator<Sketch>()
  {
    @Override
    public int compare(Sketch o, Sketch o1)
    {
      return Double.compare(o.getEstimate(), o1.getEstimate());
    }
  };

  private static final byte CACHE_TYPE_ID = 15;

  private final String name;
  private final String fieldName;
  private final String predicate;
  private final int size;
  private final boolean shouldFinalize;
  private final boolean isInputThetaSketch;
  private final Integer errorBoundsStdDev;

  @JsonCreator
  public SketchMergeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("size") Integer size,
      @JsonProperty("shouldFinalize") Boolean shouldFinalize,
      @JsonProperty("isInputThetaSketch") Boolean isInputThetaSketch,
      @JsonProperty("errorBoundsStdDev") Integer errorBoundsStdDev
  )
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");
    this.predicate = predicate;
    this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;
    this.shouldFinalize = shouldFinalize == null || shouldFinalize.booleanValue();
    this.isInputThetaSketch = isInputThetaSketch != null && isInputThetaSketch.booleanValue();
    this.errorBoundsStdDev = errorBoundsStdDev;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ValueDesc type = metricFactory.resolve(fieldName);
    if (type == null) {
      return new EmptySketchAggregator();
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, metricFactory);
    if (type.isDimension()) {
      DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return Aggregators.wrap(matcher, SketchAggregator.create(selector, size));
    }
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    return Aggregators.wrap(matcher, SketchAggregator.create(selector, size));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ValueDesc type = metricFactory.resolve(fieldName);
    if (type == null) {
      return new EmptySketchBufferAggregator();
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, metricFactory);
    int maxIntermediateSize = getMaxIntermediateSize();
    if (type.isDimension()) {
      DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return Aggregators.wrap(matcher, SketchBufferAggregator.create(selector, size, maxIntermediateSize));
    }
    ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    return Aggregators.wrap(matcher, SketchBufferAggregator.create(selector, size, maxIntermediateSize));
  }

  @Override
  public Object deserialize(Object object)
  {
    return ThetaOperations.deserialize(object);
  }

  @Override
  public Comparator<Sketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public BinaryFn.Identical combiner()
  {
    return (lhs, rhs) ->
    {
      final Union union;
      if (lhs instanceof Union) {
        union = (Union) lhs;
        updateUnion(union, rhs);
      } else if (rhs instanceof Union) {
        union = (Union) rhs;
        updateUnion(union, lhs);
      } else {
        union = (Union) SetOperation.builder().setNominalEntries(size).build(Family.UNION);
        updateUnion(union, lhs);
        updateUnion(union, rhs);
      }
      return union;
    };
  }

  private void updateUnion(Union union, Object obj)
  {
    if (obj == null) {
      return;
    } else if (obj instanceof Memory) {
      union.update((Memory) obj);
    } else if (obj instanceof Sketch) {
      union.update((Sketch) obj);
    } else if (obj instanceof Union) {
      union.update(((Union) obj).getResult(false, null));
    } else {
      throw new IAE("Object of type [%s] can not be unioned", obj.getClass().getName());
    }
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
  public String getPredicate()
  {
    return predicate;
  }

  @JsonProperty
  public int getSize()
  {
    return size;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return SetOperation.getMaxUnionBytes(size);
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new SketchMergeAggregatorFactory(name, name, null, size, shouldFinalize, false, errorBoundsStdDev);
  }

  protected boolean isMergeable(AggregatorFactory other)
  {
    return getName().equals(other.getName()) && other instanceof SketchMergeAggregatorFactory;
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (!isMergeable(other)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    final SketchMergeAggregatorFactory castedOther = (SketchMergeAggregatorFactory) other;
    return new SketchMergeAggregatorFactory(
        name,
        name,
        null,
        Math.max(size, castedOther.size),
        shouldFinalize,
        false,
        errorBoundsStdDev
    );
  }

  @JsonProperty
  public boolean getShouldFinalize()
  {
    return shouldFinalize;
  }

  @JsonProperty
  public boolean getIsInputThetaSketch()
  {
    return isInputThetaSketch;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getErrorBoundsStdDev()
  {
    return errorBoundsStdDev;
  }

  /**
   * Finalize the computation on sketch object and returns estimate from underlying
   * sketch.
   *
   * @param object the sketch object
   *
   * @return sketch object
   */
  @Override
  public Object finalizeComputation(Object object)
  {
    if (object != null && shouldFinalize) {
      final Sketch sketch = object instanceof Union ? ((Union) object).getResult() : (Sketch) object;
      if (errorBoundsStdDev == null) {
        return sketch.getEstimate();
      }
      return new SketchEstimateWithErrorBounds(
          sketch.getEstimate(),
          sketch.getUpperBound(errorBoundsStdDev),
          sketch.getLowerBound(errorBoundsStdDev),
          errorBoundsStdDev
      );
    }
    return object;
  }

  @Override
  public ValueDesc finalizedType()
  {
    if (shouldFinalize) {
      return errorBoundsStdDev != null ? ValueDesc.of("sketchEstimateWithErrorBounds") : ValueDesc.DOUBLE;
    }
    return getOutputType();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return isInputThetaSketch ? SketchModule.THETA_SKETCH_MERGE_AGG : SketchModule.THETA_SKETCH_BUILD_AGG;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName)
                  .append(size);
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

    SketchMergeAggregatorFactory that = (SketchMergeAggregatorFactory) o;

    if (size != that.size) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }
    if (shouldFinalize != that.shouldFinalize) {
      return false;
    }
    if (errorBoundsStdDev != that.errorBoundsStdDev) {
      return false;
    }
    return isInputThetaSketch == that.isInputThetaSketch;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + size;
    result = 31 * result + (shouldFinalize ? 1 : 0);
    result = 31 * result + (isInputThetaSketch ? 1 : 0);
    result = 31 * result + (errorBoundsStdDev != null ? errorBoundsStdDev.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SketchMergeAggregatorFactory{"
           + "name=" + name
           + ", fieldName=" + fieldName
           + (predicate == null ? "" : ", predicate=" + predicate)
           + ", size=" + size
           + ", shouldFinalize=" + shouldFinalize
           + ", isInputThetaSketch=" + isInputThetaSketch
           + ", errorBoundsStdDev=" + errorBoundsStdDev
           + "}";
  }
}
