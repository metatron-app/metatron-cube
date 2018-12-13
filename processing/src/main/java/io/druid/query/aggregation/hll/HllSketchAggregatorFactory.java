/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.query.aggregation.hll;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import io.druid.common.CacheKeyBuilder;
import io.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Base class for both build and merge factories
 *
 * @author Alexander Saydakov
 */
abstract class HllSketchAggregatorFactory extends AggregatorFactory
{

  static final int DEFAULT_LG_K = 12;
  static final TgtHllType DEFAULT_TGT_HLL_TYPE = TgtHllType.HLL_4;

  static final Comparator<HllSketch> COMPARATOR = Ordering.from(
      new Comparator<HllSketch>()
      {
        @Override
        public int compare(HllSketch o1, HllSketch o2)
        {
          return Doubles.compare(o1.getEstimate(), o2.getEstimate());
        }
      }).nullsFirst();

  private final String name;
  private final String fieldName;
  private final int lgK;
  private final TgtHllType tgtHllType;

  HllSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer lgK,
      @Nullable final String tgtHllType
  )
  {
    this.name = Objects.requireNonNull(name);
    this.fieldName = Objects.requireNonNull(fieldName);
    this.lgK = lgK == null ? DEFAULT_LG_K : lgK;
    this.tgtHllType = tgtHllType == null ? DEFAULT_TGT_HLL_TYPE : TgtHllType.valueOf(tgtHllType);
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
  public int getLgK()
  {
    return lgK;
  }

  @JsonProperty
  public String getTgtHllType()
  {
    return tgtHllType.toString();
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public HllSketch deserialize(final Object object)
  {
    return HllSketchMergeComplexMetricSerde.deserializeSketch(object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<HllSketch> combiner()
  {
    return new Combiner<HllSketch>()
    {
      @Override
      public HllSketch combine(HllSketch param1, HllSketch param2)
      {
        final Union union = new Union(lgK);
        union.update(param1);
        union.update(param2);
        return union.getResult(tgtHllType);
      }
    };
  }

  @Override
  public Double finalizeComputation(final Object object)
  {
    final HllSketch sketch = (HllSketch) object;
    return sketch.getEstimate();
  }

  @Override
  public Comparator<HllSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HllSketchMergeAggregatorFactory(getName(), getName(), getLgK(), getTgtHllType());
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(getCacheTypeId()).appendString(name).appendString(fieldName)
                                                .appendInt(lgK).appendInt(tgtHllType.ordinal()).build();
  }

  @Override
  public boolean equals(final Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().equals(object.getClass())) {
      return false;
    }
    final HllSketchAggregatorFactory that = (HllSketchAggregatorFactory) object;
    if (!name.equals(that.getName())) {
      return false;
    }
    if (!fieldName.equals(that.getFieldName())) {
      return false;
    }
    if (lgK != that.getLgK()) {
      return false;
    }
    if (!tgtHllType.equals(that.tgtHllType)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, lgK, tgtHllType);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + " {"
           + "name=" + name
           + ", fieldName=" + fieldName
           + ", lgK=" + lgK
           + ", tgtHllType=" + tgtHllType
           + "}";
  }

  protected abstract byte getCacheTypeId();

}
