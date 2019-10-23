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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.select.SelectResultValue;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * just for test
 */
@JsonTypeName("selectEach")
public class SelectEachQuery extends AbstractIteratingQuery<Result<SelectResultValue>, Result<SelectResultValue>>
{
  private int index;
  private final List<Interval> intervals;

  public SelectEachQuery(
      @JsonProperty("select") Query<Result<SelectResultValue>> query,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query, context);
    intervals = query.getIntervals();
  }

  @Override
  protected Query<Result<SelectResultValue>> newInstance(
      Query<Result<SelectResultValue>> query, Map<String, Object> context
  )
  {
    return new SelectEachQuery(query, context);
  }

  @Override
  public Pair<Sequence<Result<SelectResultValue>>, Query<Result<SelectResultValue>>> next(
      Sequence<Result<SelectResultValue>> sequence,
      Query<Result<SelectResultValue>> prev
      )
  {
    if (index < intervals.size()) {
      return Pair.of(
          sequence,
          prev.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Arrays.asList(intervals.get(index++))))
      );
    }
    return Pair.of(sequence, null);
  }
}
