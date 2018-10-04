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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.select.Schema;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class PostAggregationsPostProcessor
    extends PostProcessingOperator.Abstract
    implements PostProcessingOperator.SchemaResolving
{
  private final List<PostAggregator> postAggregations;

  @JsonCreator
  public PostAggregationsPostProcessor(
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations
  )
  {
    this.postAggregations = postAggregations == null ? ImmutableList.<PostAggregator>of() : postAggregations;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<PostAggregator> getPostAggregations()
  {
    return postAggregations;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    if (postAggregations.isEmpty()) {
      return baseRunner;
    }
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final Sequence<Row> sequence = Queries.convertToRow(query, baseRunner.run(query, responseContext));
        return Queries.convertBack(
            query, Sequences.map(
                sequence, new Function<Row, Row>()
                {
                  @Override
                  public Row apply(Row input)
                  {
                    Map<String, Object> event = Rows.asMap(input);
                    for (PostAggregator postAggregator : postAggregations) {
                      event.put(postAggregator.getName(), postAggregator.compute(input.getTimestamp(), event));
                    }
                    return new MapBasedRow(input.getTimestamp(), event);
                  }
                }
            )
        );
      }
    };
  }

  @Override
  public IncrementalIndexSchema resolve(Query query, IncrementalIndexSchema input, ObjectMapper mapper)
  {
    if (GuavaUtils.isNullOrEmpty(postAggregations)) {
      return input;
    }
    Schema schema = input.asSchema(true);
    List<String> dimensionNames = input.getDimensionsSpec().getDimensionNames();
    List<String> metricNames = input.getMetricNames();
    List<AggregatorFactory> aggregatorFactories = Lists.newArrayList(Arrays.asList(input.getMetrics()));
    for (PostAggregator postAggregator : postAggregations) {
      String outputName = postAggregator.getName();
      if (dimensionNames.indexOf(outputName) >= 0) {
        continue; // whatever it is, it's string
      }
      ValueDesc valueDesc = postAggregator.resolve(schema);
      if (metricNames.indexOf(outputName) >= 0) {
        aggregatorFactories.set(
            metricNames.indexOf(outputName),
            new RelayAggregatorFactory(outputName, valueDesc.typeName())
        );
      } else {
        aggregatorFactories.add(
            metricNames.indexOf(outputName),
            new RelayAggregatorFactory(outputName, valueDesc.typeName())
        );
      }
    }
    return input.withMetrics(aggregatorFactories.toArray(new AggregatorFactory[0]));
  }
}
