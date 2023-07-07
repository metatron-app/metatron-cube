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

package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("digestQuantileAgg")
public class DruidTDigestAggregatorFactory extends AggregatorFactory
{
  public static final ValueDesc TYPE = ValueDesc.of("DruidTDigest");

  private static final byte CACHE_TYPE_ID = 0xA;

  protected final String name;
  protected final String fieldName;
  protected final int compression;

  public DruidTDigestAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("compression") Integer compression
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.compression = compression == null ? DruidTDigestAggregator.DEFAULT_COMPRESSION : compression;

    Preconditions.checkArgument(this.compression > 0, "compression must be greater than 0");
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory) {
    return new DruidTDigestAggregator(
        metricFactory.makeObjectColumnSelector(fieldName),
        compression
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
    return new DruidTDigestBufferAggregator(
        metricFactory.makeObjectColumnSelector(fieldName),
        compression
    );
  }

  @Override
  public Comparator getComparator() {
    return DruidTDigestAggregator.COMPARATOR;
  }

  @Override
  public BinaryFn.Identical<DruidTDigest> combiner()
  {
    return (param1, param2) -> param1.add(param2);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DruidTDigestAggregatorFactory(name, name, compression);
  }

  @Override
  public Object deserialize(Object object) {
    if (object instanceof byte[]) {
      return DruidTDigest.fromBytes(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return DruidTDigest.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      byte[] bytes = StringUtils.decodeBase64((String) object);
      return DruidTDigest.fromBytes(ByteBuffer.wrap(bytes));
    } else {
      return object;
    }
  }

  @Override
  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public int getCompression()
  {
    return compression;
  }

  @Override
  public List<String> requiredFields() {
    return Arrays.asList(fieldName);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldName)
                  .append(compression);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return TYPE;
  }

  @Override
  public int getMaxIntermediateSize() {
    // NOTE: compression threshold is set as compression * 100 in TDigest
    return DruidTDigest.maxStorageSize(compression);
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

    DruidTDigestAggregatorFactory that = (DruidTDigestAggregatorFactory) o;

    if (compression != that.compression) {
      return false;
    }
    if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
    result = 31 * result + compression;

    return result;
  }

  @Override
  public String toString()
  {
    return "ApproximateQuantileAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", compression=" + compression +
           '}';
  }
}
