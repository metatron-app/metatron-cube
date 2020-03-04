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

package io.druid.query.aggregation.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Returns a union of a given list of sketches.
 * @author Alexander Saydakov
 */
public class HllSketchUnionPostAggregator extends PostAggregator.Abstract
{

  private final String name;
  private final List<PostAggregator> fields;
  private final int lgK;
  private final TgtHllType tgtHllType;

  @JsonCreator
  public HllSketchUnionPostAggregator(
      @JsonProperty("name") final String name,
      @JsonProperty("fields") final List<PostAggregator> fields,
      @JsonProperty("lgK") @Nullable final Integer lgK,
      @JsonProperty("tgtHllType") @Nullable final String tgtHllType
  )
  {
    this.name = name;
    this.fields = fields;
    this.lgK = lgK == null ? HllSketchAggregatorFactory.DEFAULT_LG_K : lgK;
    this.tgtHllType = tgtHllType == null ? HllSketchAggregatorFactory.DEFAULT_TGT_HLL_TYPE
        : TgtHllType.valueOf(tgtHllType);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<PostAggregator> getFields()
  {
    return fields;
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
  public Set<String> getDependentFields()
  {
    final Set<String> dependentFields = new LinkedHashSet<>();
    for (PostAggregator field : fields) {
      dependentFields.addAll(field.getDependentFields());
    }
    return dependentFields;
  }

  @Override
  public Comparator<HllSketch> getComparator()
  {
    return HllSketchAggregatorFactory.COMPARATOR;
  }

  @Override
  public Processor processor()
  {
    return new AbstractProcessor()
    {
      private final List<Processor> processors = PostAggregators.toProcessors(fields);

      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        final Union union = new Union(lgK);
        for (Processor processor : processors) {
          union.update((HllSketch) processor.compute(timestamp, combinedAggregators));
        }
        return union.getResult(tgtHllType);
      }
    };
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.of(HllSketchModule.TYPE_NAME);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
        "name='" + name + '\'' +
        ", fields=" + fields +
        ", lgK=" + lgK +
        ", tgtHllType=" + tgtHllType +
        "}";
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HllSketchUnionPostAggregator)) {
      return false;
    }

    final HllSketchUnionPostAggregator that = (HllSketchUnionPostAggregator) o;

    if (!name.equals(that.name)) {
      return false;
    }
    if (!fields.equals(that.fields)) {
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
    return Objects.hash(name, fields, lgK, tgtHllType);
  }
}
