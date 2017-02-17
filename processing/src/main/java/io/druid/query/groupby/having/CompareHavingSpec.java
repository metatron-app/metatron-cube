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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public abstract class CompareHavingSpec implements HavingSpec
{
  protected final String aggregationName;
  protected final Number value;

  public CompareHavingSpec(String aggName, Number value)
  {
    this.aggregationName = Preconditions.checkNotNull(aggName, "aggregation cannot be null");
    this.value = value;
  }

  @JsonProperty("aggregation")
  public String getAggregationName()
  {
    return aggregationName;
  }

  @JsonProperty("value")
  public Number getValue()
  {
    return value;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] aggBytes = StringUtils.toUtf8(aggregationName);
    final byte[] valBytes = Bytes.toArray(Arrays.asList(value));
    return ByteBuffer.allocate(1 + aggBytes.length + valBytes.length)
                     .put(getCacheType())
                     .put(aggBytes)
                     .put(valBytes)
                     .array();
  }

  protected abstract byte getCacheType();

  /**
   * This method treats internal value as double mainly for ease of test.
   */
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CompareHavingSpec that = (CompareHavingSpec) o;

    if (!aggregationName.equals(that.aggregationName)) {
      return false;
    }
    if (value != null && that.value != null) {
      return Double.compare(value.doubleValue(), that.value.doubleValue()) == 0;
    }

    if (value == null && that.value == null) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode()
  {
    int result = aggregationName != null ? aggregationName.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append("{aggregationName='").append(aggregationName).append('\'');
    sb.append(", value=").append(value);
    sb.append('}');
    return sb.toString();
  }
}
