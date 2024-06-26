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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("stddev")
public class StandardDeviationPostAggregator extends PostAggregator.Stateless implements PostAggregator.SQLSupport
{
  protected final String name;
  protected final String fieldName;
  protected final String estimator;

  protected final boolean isVariancePop;

  @JsonCreator
  public StandardDeviationPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("estimator") String estimator
  )
  {
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName is null");
    this.name = Preconditions.checkNotNull(name, "name is null");
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator<Double> getComparator()
  {
    return GuavaUtils.nullFirstNatural();
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  protected Processor createStateless()
  {
    return new AbstractProcessor()
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        final VarianceAggregatorCollector collector = (VarianceAggregatorCollector) combinedAggregators.get(fieldName);
        if (collector == null) {
          return null;
        }
        final Double variance = collector.getVariance(isVariancePop);
        return variance == null ? null : Math.sqrt(variance);
      }
    };
  }

  @Override
  public PostAggregator rewrite(String name, String fieldName)
  {
    return new StandardDeviationPostAggregator(name, fieldName, estimator);
  }

  @Override
  @JsonProperty("name")
  public String getName()
  {
    return name;
  }

  @JsonProperty("fieldName")
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty("estimator")
  public String getEstimator()
  {
    return estimator;
  }

  @Override
  public String toString()
  {
    return "StandardDeviationPostAggregator{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", isVariancePop='" + isVariancePop + '\'' +
           '}';
  }
}
