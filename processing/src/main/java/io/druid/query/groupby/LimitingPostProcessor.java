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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.common.guava.Sequence;
import io.druid.data.input.Row;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.groupby.orderby.LimitSpec;

import java.util.Map;

/**
 */
@JsonTypeName("limit")
public class LimitingPostProcessor extends PostProcessingOperator.ReturnsRow<Row> implements PostProcessingOperator.Local
{
  private final LimitSpec limitSpec;
  private final Supplier<GroupByQueryConfig> groupByConfig;

  @JsonCreator
  public LimitingPostProcessor(
      @JsonProperty("limitSpec") LimitSpec limitSpec,
      @JacksonInject Supplier<GroupByQueryConfig> groupByConfig
  )
  {
    this.limitSpec = Preconditions.checkNotNull(limitSpec);
    this.groupByConfig = Preconditions.checkNotNull(groupByConfig);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Row> postProcess(final QueryRunner<Row> baseQueryRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        final Query representative = BaseQuery.getRepresentative(query);
        Preconditions.checkArgument(representative instanceof BaseAggregationQuery, "only accepts aggregation query");
        final BaseAggregationQuery aggregation = ((BaseAggregationQuery) representative).withLimitSpec(limitSpec);
        return aggregation.applyLimit(
            baseQueryRunner.run(query, responseContext),
            aggregation.isSortOnTimeForLimit(groupByConfig.get().isSortOnTime())
        );
      }
    };
  }

  @JsonProperty
  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  @Override
  public String toString()
  {
    return "LimitingPostProcessor{" +
           "limitSpec=" + limitSpec +
           '}';
  }
}
