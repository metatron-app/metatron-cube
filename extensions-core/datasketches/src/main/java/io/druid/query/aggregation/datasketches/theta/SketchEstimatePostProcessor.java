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
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
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
        if (query instanceof GroupByQuery) {
          return Sequences.map(
              sequence, new Function<Row, Row>()
              {
                @Override
                public Row apply(Row input)
                {
                  Row.Updatable updatable = Rows.toUpdatable(input);
                  for (String column : targetColumns) {
                    Sketch sketch = (Sketch) updatable.getRaw(column);
                    updatable.set(column, sketch.getEstimate());
                    updatable.set(column + ".estimation", sketch.isEstimationMode());
                    if (sketch.isEstimationMode()) {
                      updatable.set(column + ".upper95", sketch.getUpperBound(2));
                      updatable.set(column + ".lower95", sketch.getLowerBound(2));
                    }
                  }
                  return (Row) updatable;
                }
              }
          );
        } else if (query instanceof TimeseriesQuery) {
          return Sequences.map(
              sequence, new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
              {
                @Override
                public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> input)
                {
                  TimeseriesResultValue resultValue = input.getValue();
                  if (MapBasedRow.supportInplaceUpdate(resultValue.getBaseObject())) {
                    Map<String, Object> updatable = resultValue.getBaseObject();
                    for (String column : targetColumns) {
                      Sketch sketch = (Sketch) updatable.get(column);
                      updatable.put(column, sketch.getEstimate());
                      updatable.put(column + ".estimation", sketch.isEstimationMode());
                      if (sketch.isEstimationMode()) {
                        updatable.put(column + ".upper95", sketch.getUpperBound(2));
                        updatable.put(column + ".lower95", sketch.getLowerBound(2));
                      }
                    }
                    return input;
                  }
                  Map<String, Object> updatable = Maps.newHashMap(resultValue.getBaseObject());
                  for (String column : targetColumns) {
                    Sketch sketch = (Sketch) updatable.get(column);
                    updatable.put(column, sketch.getEstimate());
                    updatable.put(column + ".estimation", sketch.isEstimationMode());
                    if (sketch.isEstimationMode()) {
                      updatable.put(column + ".upper95", sketch.getUpperBound(2));
                      updatable.put(column + ".lower95", sketch.getLowerBound(2));
                    }
                  }
                  return new Result<TimeseriesResultValue>(input.getTimestamp(), new TimeseriesResultValue(updatable));
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
                    for (String column : targetColumns) {
                      Sketch sketch = (Sketch) updatable.get(column);
                      updatable.put(column, sketch.getEstimate());
                      updatable.put(column + ".estimation", sketch.isEstimationMode());
                      if (sketch.isEstimationMode()) {
                        updatable.put(column + ".upper95", sketch.getUpperBound(2));
                        updatable.put(column + ".lower95", sketch.getLowerBound(2));
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
