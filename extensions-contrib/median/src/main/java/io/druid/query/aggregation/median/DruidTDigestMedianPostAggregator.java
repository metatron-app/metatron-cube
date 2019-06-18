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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("digestMedian")
public class DruidTDigestMedianPostAggregator implements PostAggregator
{

  private String name;
  private String fieldName;

  @JsonCreator
  public DruidTDigestMedianPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Comparator getComparator()
  {
    return DruidTDigestAggregator.COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public Double compute(DateTime timestamp, Map<String, Object> values)
  {
    final DruidTDigest digest = (DruidTDigest) values.get(this.getFieldName());
    return digest.median();
  }

  @Override
  public String getName()
  {
    return name;
  }

  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "DruidTDigestMedianPostAggregator{" +
        "fieldName='" + fieldName + '\'' +
        '}';
  }
}