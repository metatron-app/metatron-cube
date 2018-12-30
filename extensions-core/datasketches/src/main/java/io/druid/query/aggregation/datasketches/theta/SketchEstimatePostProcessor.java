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

package io.druid.query.aggregation.datasketches.theta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("sketch.estimate")
public class SketchEstimatePostProcessor extends PostProcessingOperator.Abstract
{
  @JsonCreator
  public SketchEstimatePostProcessor() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner baseQueryRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        final Set<String> targetColumns = Sets.newHashSet();
        for (AggregatorFactory aggregator : Queries.getAggregators(query)) {
          if (aggregator instanceof SketchMergeAggregatorFactory) {
            if (!((SketchMergeAggregatorFactory)aggregator).getShouldFinalize()) {
              targetColumns.add(aggregator.getName());
            }
          }
        }
        for (PostAggregator aggregator : Queries.getPostAggregators(query)) {
          targetColumns.remove(aggregator.getName());
        }
        Sequence sequence = baseQueryRunner.run(query, responseContext);
        if (targetColumns.isEmpty()) {
          return sequence;
        }
        final boolean complexColumns;
        if (query instanceof BaseAggregationQuery) {
          complexColumns = !GuavaUtils.isNullOrEmpty(((BaseAggregationQuery)query).getLimitSpec().getWindowingSpecs());
        } else {
          complexColumns = false;
        }
        if (query instanceof BaseAggregationQuery) {
          return Sequences.map(
              sequence, new Function<Row, Row>()
              {
                @Override
                public Row apply(Row input)
                {
                  Row.Updatable updatable = Rows.toUpdatable(input);
                  Iterable<String> columns = complexColumns ? Lists.newArrayList(input.getColumns()) : targetColumns;
                  for (String column : columns) {
                    Object value = updatable.getRaw(column);
                    if (value instanceof Sketch || value instanceof Union) {
                      Sketch sketch = value instanceof Sketch ? (Sketch) value : ((Union) value).getResult();
                      updatable.set(column, sketch.getEstimate());
                      updatable.set(column + ".estimation", sketch.isEstimationMode());
                      if (sketch.isEstimationMode()) {
                        updatable.set(column + ".upper95", sketch.getUpperBound(2));
                        updatable.set(column + ".lower95", sketch.getLowerBound(2));
                      }
                    }
                  }
                  return updatable;
                }
              }
          );
        } else if (query instanceof TopNQuery) {
          return Sequences.map(
              sequence, new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
              {
                @Override
                public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                {
                  List<Map<String, Object>> source = input.getValue().getValue();
                  List<Map<String, Object>> result = Lists.newArrayListWithExpectedSize(source.size());
                  for (Map<String, Object> row : source) {
                    Map<String, Object> updatable = MapBasedRow.supportInplaceUpdate(row) ? row : Maps.newHashMap(row);
                    Iterable<String> columns = complexColumns ? Lists.newArrayList(updatable.keySet()) : targetColumns;
                    for (String column : columns) {
                      Object value = updatable.get(column);
                      if (value instanceof Sketch || value instanceof Union) {
                        Sketch sketch = value instanceof Sketch ? (Sketch) value : ((Union) value).getResult();
                        updatable.put(column, sketch.getEstimate());
                        updatable.put(column + ".estimation", sketch.isEstimationMode());
                        if (sketch.isEstimationMode()) {
                          updatable.put(column + ".upper95", sketch.getUpperBound(2));
                          updatable.put(column + ".lower95", sketch.getLowerBound(2));
                        }
                      }
                    }
                    result.add(updatable);
                  }
                  return new Result<TopNResultValue>(input.getTimestamp(), new TopNResultValue(result));
                }
              }
          );
        }
        return sequence;
      }
    };
  }
}
