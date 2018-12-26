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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.select.Schema;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RowMappingPostProcessor
    extends PostProcessingOperator.Abstract
    implements PostProcessingOperator.SchemaResolving
{
  private final Map<String, String> mapping;

  @JsonCreator
  public RowMappingPostProcessor(@JsonProperty Map<String, String> mapping)
  {
    this.mapping = mapping;
  }

  @JsonProperty
  public Map<String, String> getMapping()
  {
    return mapping;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        return Sequences.map(baseRunner.run(query, responseContext), new Function<Row, Row>()
        {
          @Override
          public Row apply(Row input)
          {
            Row.Updatable updatable = Rows.toUpdatable(input);
            for (Map.Entry<String, String> entry : mapping.entrySet()) {
              updatable.set(entry.getKey(), updatable.remove(entry.getValue()));
            }
            return updatable;
          }
        });
      }
    };
  }

  @Override
  public IncrementalIndexSchema resolve(Query query, IncrementalIndexSchema input, ObjectMapper mapper)
  {
    if (GuavaUtils.isNullOrEmpty(mapping)) {
      return input;
    }
    Schema schema = input.asSchema(true);
    List<String> dimensionNames = input.getDimensionsSpec().getDimensionNames();
    List<DimensionSchema> dimensionSchemas = input.getDimensionsSpec().getDimensions();
    List<String> metricNames = input.getMetricNames();
    List<AggregatorFactory> aggregatorFactories = Lists.newArrayList(Arrays.asList(input.getMetrics()));
    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      final String from = entry.getValue();
      final String to = entry.getKey();
      int findex = dimensionNames.indexOf(from);
      if (findex >= 0) {
        dimensionNames.remove(findex);
        DimensionSchema removed = dimensionSchemas.remove(findex);
        if (removed != null) {
          int tindex = dimensionNames.indexOf(to);
          if (tindex >= 0) {
            dimensionSchemas.set(tindex, removed);
            dimensionNames.set(tindex, removed.getName());
            continue;
          }
          tindex = metricNames.indexOf(to);
          if (tindex >= 0) {
            metricNames.set(tindex, to);
            aggregatorFactories.add(tindex, new RelayAggregatorFactory(to, ValueDesc.STRING));
          } else {
            metricNames.add(to);
            aggregatorFactories.add(new RelayAggregatorFactory(to, ValueDesc.STRING));
          }
        }
        continue;
      }
      findex = metricNames.indexOf(from);
      if (findex >= 0) {
        metricNames.remove(findex);
        AggregatorFactory removed = aggregatorFactories.remove(findex);
        if (removed != null) {
          int tindex = dimensionNames.indexOf(to);
          if (tindex >= 0) {
            // whatever it is, it's string
            continue;
          }
          tindex = metricNames.indexOf(to);
          if (tindex >= 0) {
            metricNames.set(tindex, to);
            aggregatorFactories.add(tindex, removed);
          } else {
            metricNames.add(to);
            aggregatorFactories.add(removed);
          }
        }
      }
    }
    return input.withDimensionsSpec(new DimensionsSpec(dimensionSchemas, null, null))
                .withMetrics(aggregatorFactories.toArray(new AggregatorFactory[0]));
  }
}
