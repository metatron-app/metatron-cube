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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class HistogramAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 0x7;

  private final String name;
  private final String fieldName;
  private final List<Float> breaksList;

  private final float[] breaks;

  @JsonCreator
  public HistogramAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("breaks") final List<Float> breaksList
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.breaksList = (breaksList == null) ? Lists.<Float>newArrayList() : breaksList;
    this.breaks = new float[this.breaksList.size()];
    for (int i = 0; i < this.breaksList.size(); ++i) {
      this.breaks[i] = this.breaksList.get(i);
    }
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new HistogramAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        breaks
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new HistogramBufferAggregator(
        metricFactory.makeFloatColumnSelector(fieldName),
        breaks
    );
  }

  @Override
  public Comparator getComparator()
  {
    return HistogramAggregator.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<Histogram> combiner()
  {
    return new Combiner<Histogram>()
    {
      @Override
      public Histogram combine(Histogram param1, Histogram param2)
      {
        return param1.fold(param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HistogramAggregatorFactory(name, name, breaksList);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return Histogram.fromBytes((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      return Histogram.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      byte[] bytes = StringUtils.decodeBase64((String) object);
      return Histogram.fromBytes(bytes);
    }
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object == null ? null : ((Histogram) object).asVisual();
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.of("histogramVisual");
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
    return "histogram";
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
    return new HistogramAggregatorFactory(name, inputField, breaksList);
  }

  @JsonProperty
  public List<Float> getBreaks()
  {
    return breaksList;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName)
                  .append(breaks);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("histogram");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES * (breaks.length + 1) + Float.BYTES * 2;
  }

  @Override
  public String toString()
  {
    return "HistogramAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", breaks=" + Arrays.toString(breaks) +
           '}';
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

    HistogramAggregatorFactory that = (HistogramAggregatorFactory) o;

    if (!Arrays.equals(breaks, that.breaks)) {
      return false;
    }
    if (!Objects.equals(breaksList, that.breaksList)) {
      return false;
    }
    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + (breaksList != null ? breaksList.hashCode() : 0);
    result = 31 * result + (breaks != null ? Arrays.hashCode(breaks) : 0);
    return result;
  }
}
