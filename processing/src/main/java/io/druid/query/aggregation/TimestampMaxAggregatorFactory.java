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
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.segment.ColumnSelectorFactory;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TimestampMaxAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = 31;

  private final String name;
  private final String fieldName;
  private final String timeFormat;

  private final DefaultTimestampSpec timestampSpec;

  @JsonCreator
  public TimestampMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("timeFormat") String timeFormat
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
    this.timeFormat = timeFormat;
    this.timestampSpec = new DefaultTimestampSpec(fieldName, timeFormat, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new TimestampMaxAggregator(metricFactory.makeObjectColumnSelector(fieldName), timestampSpec);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new TimestampMaxBufferAggregator(metricFactory.makeObjectColumnSelector(fieldName), timestampSpec);
  }

  @Override
  public Comparator getComparator()
  {
    return TimestampMaxAggregator.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<Number> combiner()
  {
    return new Combiner.Abstract<Number>()
    {
      @Override
      protected final Number _combine(Number param1, Number param2)
      {
        return Math.max(param1.longValue(), param2.longValue());
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new TimestampMaxAggregatorFactory(name, name, timeFormat);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object instanceof Number ? new DateTime(((Number) object).longValue()) : null;
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.DATETIME;
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
    return "timeMax";
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
    return new TimestampMaxAggregatorFactory(name, inputField, timeFormat);
  }

  @JsonProperty
  public String getTimeFormat()
  {
    return timeFormat;
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
                  .append(fieldName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public String toString()
  {
    return "TimestampMaxAggregatorFactory{name='" + name + '\'' + ", fieldName='" + fieldName + '\'' + '}';
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

    TimestampMaxAggregatorFactory that = (TimestampMaxAggregatorFactory) o;

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
    int result = fieldName.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }
}
