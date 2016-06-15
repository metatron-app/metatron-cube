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

package io.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.query.Cacheable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopLimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultLimitSpec.class)
})
public interface LimitSpec extends Cacheable
{
  public List<WindowingSpec> getWindowingSpecs();

  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions,
      List<AggregatorFactory> aggs,
      List<PostAggregator> postAggs
  );

  public LimitSpec merge(LimitSpec other);

  Function<Sequence<Row>, List<Row>> SEQUENCE_TO_LIST = new Function<Sequence<Row>, List<Row>>()
  {
    @Override
    public List<Row> apply(Sequence<Row> input)
    {
      return Sequences.toList(input, Lists.<Row>newArrayList());
    }
  };

  Function<List<Row>, Sequence<Row>> LIST_TO_SEQUENCE = new Function<List<Row>, Sequence<Row>>()
  {
    @Override
    public Sequence<Row> apply(List<Row> input)
    {
      return Sequences.simple(input);
    }
  };
}
