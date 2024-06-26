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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("postAggregations")
public class PostAggregationsPostProcessor extends PostProcessingOperator.ReturnsRow<Row>
    implements RowSignature.Evolving
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
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<PostAggregator> getPostAggregations()
  {
    return postAggregations;
  }

  @Override
  public QueryRunner<Row> postProcess(final QueryRunner<Row> baseRunner)
  {
    if (postAggregations.isEmpty()) {
      return baseRunner;
    }
    return new QueryRunner<Row>()
    {
      private final List<PostAggregator.Processor> postProcessors = PostAggregators.toProcessors(postAggregations);
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query query, Map responseContext)
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
                    for (PostAggregator.Processor postAggregator : postProcessors) {
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
  public List<String> evolve(List<String> schema)
  {
    if (GuavaUtils.isNullOrEmpty(postAggregations)) {
      return schema;
    }
    for (PostAggregator postAggregator : postAggregations) {
      String outputName = postAggregator.getName();
      if (!schema.contains(outputName)) {
        schema.add(outputName);
      }
    }
    return schema;
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    if (schema == null || GuavaUtils.isNullOrEmpty(postAggregations)) {
      return schema;
    }
    RowSignature.Builder builder = new RowSignature.Builder(schema);
    for (PostAggregator postAggregator : postAggregations) {
      builder.override(postAggregator.getName(), postAggregator.resolve(schema));
    }
    return builder.build();
  }

  @Override
  public String toString()
  {
    return "PostAggregationsPostProcessor{" +
           "postAggregations=" + postAggregations +
           '}';
  }
}
