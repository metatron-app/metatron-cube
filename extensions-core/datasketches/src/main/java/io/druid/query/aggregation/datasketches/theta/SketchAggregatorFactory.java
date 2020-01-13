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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.sketch.ThetaOperations;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class SketchAggregatorFactory extends AggregatorFactory
{
  public static final int DEFAULT_MAX_SKETCH_SIZE = 16384;

  protected final String name;
  protected final String fieldName;
  protected final int size;
  private final byte cacheId;

  public static final Comparator<Sketch> COMPARATOR = new Comparator<Sketch>()
  {
    @Override
    public int compare(Sketch o, Sketch o1)
    {
      return Doubles.compare(o.getEstimate(), o1.getEstimate());
    }
  };

  public SketchAggregatorFactory(String name, String fieldName, Integer size, byte cacheId)
  {
    this.name = Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.size = size == null ? DEFAULT_MAX_SKETCH_SIZE : size;
    Util.checkIfPowerOf2(this.size, "size");

    this.cacheId = cacheId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    ValueDesc type = metricFactory.resolve(fieldName);
    if (type == null) {
      return new EmptySketchAggregator();
    }
    if (ValueDesc.isDimension(type)) {
      return SketchAggregator.create(metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName)), size);
    }
    return SketchAggregator.create(metricFactory.makeObjectColumnSelector(fieldName), size);
  }

  @SuppressWarnings("unchecked")
  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    ValueDesc type = metricFactory.resolve(fieldName);
    if (type == null) {
      return new EmptySketchBufferAggregator();
    }
    int maxIntermediateSize = getMaxIntermediateSize();
    if (ValueDesc.isDimension(type)) {
      DimensionSelector selector = metricFactory.makeDimensionSelector(DefaultDimensionSpec.of(fieldName));
      return SketchBufferAggregator.create(selector, size, maxIntermediateSize);
    }
    return SketchBufferAggregator.create(metricFactory.makeObjectColumnSelector(fieldName), size, maxIntermediateSize);
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
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner()
    {
      @Override
      public Object combine(Object lhs, Object rhs)
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
      }
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
  public byte[] getCacheKey()
  {
    return KeyBuilder.get()
                     .append(cacheId)
                     .append(fieldName)
                     .append(size)
                     .build();
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "fieldName='" + fieldName + '\''
           + ", name='" + name + '\''
           + ", size=" + size
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

    SketchAggregatorFactory that = (SketchAggregatorFactory) o;

    if (size != that.size) {
      return false;
    }
    if (cacheId != that.cacheId) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return fieldName.equals(that.fieldName);

  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + size;
    result = 31 * result + (int) cacheId;
    return result;
  }
}
