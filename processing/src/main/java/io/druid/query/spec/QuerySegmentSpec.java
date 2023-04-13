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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.common.Cacheable;
import io.druid.common.Intervals;
import io.druid.common.KeyBuilder;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacySegmentSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "legacy", value = LegacySegmentSpec.class),
    @JsonSubTypes.Type(name = "intervals", value = MultipleIntervalSegmentSpec.class),
    @JsonSubTypes.Type(name = "segments", value = MultipleSpecificSegmentSpec.class),
    @JsonSubTypes.Type(name = "segment", value = SpecificSegmentSpec.class),
    @JsonSubTypes.Type(name = "expression", value = IntervalExpressionQuerySpec.class),
    @JsonSubTypes.Type(name = "dense", value = DenseSegmentsSpec.class),
})
public interface QuerySegmentSpec extends Cacheable
{
  QuerySegmentSpec ETERNITY = MultipleIntervalSegmentSpec.of(Intervals.ETERNITY);

  List<Interval> getIntervals();

  @Override
  default KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.disable();
  }

  <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker);
}
