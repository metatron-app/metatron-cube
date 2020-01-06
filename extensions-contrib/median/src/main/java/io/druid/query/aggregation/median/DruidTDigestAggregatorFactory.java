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
import com.google.common.primitives.Ints;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("digestQuantileAgg")
public class DruidTDigestAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
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
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner<DruidTDigest>() {

      @Override
      public DruidTDigest combine(DruidTDigest param1, DruidTDigest param2)
      {
        param1.add(param2);
        return param1;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DruidTDigestAggregatorFactory(name, name, compression);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if(other.getName().equals(getName()) && other instanceof DruidTDigestAggregatorFactory) {
      return new DruidTDigestAggregatorFactory(
          name, name, compression
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public Object deserialize(Object object) {
    if (object instanceof byte[]) {
      return DruidTDigest.fromBytes(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return DruidTDigest.fromBytes((ByteBuffer) object);
    } else if (object instanceof String) {
      byte[] bytes = Base64.decodeBase64(StringUtils.getBytesUtf8((String) object));
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

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new DruidTDigestAggregatorFactory(name, inputField, compression);
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
  public byte[] getCacheKey() {
    byte[] fieldNameBytes = StringUtils.getBytesUtf8(fieldName);
    return ByteBuffer.allocate(1 + fieldNameBytes.length + Ints.BYTES)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .putInt(compression).array();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("DruidTDigest");
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
