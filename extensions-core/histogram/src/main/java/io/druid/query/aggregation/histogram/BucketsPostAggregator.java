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

package io.druid.query.aggregation.histogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;

@JsonTypeName("buckets")
public class BucketsPostAggregator extends ApproximateHistogramPostAggregator
{
  private final float bucketSize;
  private final float offset;

  private String fieldName;

  @JsonCreator
  public BucketsPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("bucketSize") float bucketSize,
      @JsonProperty("offset") float offset
  )
  {
    super(name, fieldName);
    this.bucketSize = bucketSize;
    if (this.bucketSize <= 0) {
      throw new IAE("Illegal bucketSize [%s], must be > 0", this.bucketSize);
    }
    this.offset = offset;
    this.fieldName = fieldName;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.UNKNOWN;   // has no complex serde
  }

  @Override
  public Object compute(DateTime timestamp, Map<String, Object> values)
  {
    ApproximateHistogramHolder ah = (ApproximateHistogramHolder) values.get(this.getFieldName());
    return ah.toHistogram(bucketSize, offset);
  }

  @JsonProperty
  public float getBucketSize()
  {
    return bucketSize;
  }

  @JsonProperty
  public float getOffset()
  {
    return bucketSize;
  }

  @Override
  public String toString()
  {
    return "BucketsPostAggregator{" +
           "name='" + this.getName() + '\'' +
           ", fieldName='" + this.getFieldName() + '\'' +
           ", bucketSize=" + this.getBucketSize() +
           ", offset=" + this.getOffset() +
           '}';
  }
}
