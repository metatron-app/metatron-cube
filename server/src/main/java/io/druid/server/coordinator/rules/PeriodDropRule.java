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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Objects;

/**
 */
public class PeriodDropRule extends DropRule
{
  private final Period period;
  private final Boolean includeFuture;

  @JsonCreator
  public PeriodDropRule(
      @JsonProperty("period") Period period,
      @JsonProperty("includeFuture") Boolean includeFuture
  )
  {
    this.includeFuture = includeFuture;
    this.period = period;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "dropByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean getIncludeFuture()
  {
    return includeFuture;
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    final Interval invalid = new Interval(period, referenceTimestamp);
    if (includeFuture == null || includeFuture) {
      return invalid.getStartMillis() < interval.getStartMillis();
    }
    return invalid.contains(interval);
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof PeriodDropRule &&
           Objects.equals(period, ((PeriodDropRule) other).period) &&
           Objects.equals(includeFuture, ((PeriodDropRule) other).includeFuture);
  }
}
