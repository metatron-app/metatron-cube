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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.Sequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.query.select.Schema;

import java.util.List;
import java.util.Map;

public class RowMappingPostProcessor extends PostProcessingOperator.ReturnsRow<Row>
    implements Schema.SchemaResolving
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
  public QueryRunner<Row> postProcess(final QueryRunner<Row> baseRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query query, Map responseContext)
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
  public List<String> resolve(List<String> schema)
  {
    if (GuavaUtils.isNullOrEmpty(mapping)) {
      return schema;
    }
    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      final String from = entry.getValue();
      final String to = entry.getKey();
      int findex = schema.indexOf(from);
      if (findex >= 0) {
        schema.set(findex, to);
      }
    }
    return schema;
  }

  @Override
  public Schema resolve(Query query, Schema schema, ObjectMapper mapper)
  {
    if (GuavaUtils.isNullOrEmpty(mapping)) {
      return schema;
    }
    List<String> dimensionNames = Lists.newArrayList(schema.getDimensionNames());
    List<String> metricNames = Lists.newArrayList(schema.getMetricNames());
    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      final String from = entry.getValue();
      final String to = entry.getKey();
      int findex = dimensionNames.indexOf(from);
      if (findex >= 0) {
        dimensionNames.set(findex, to);
        continue;
      }
      findex = metricNames.indexOf(from);
      if (findex >= 0) {
        metricNames.set(findex, to);
      }
    }
    return new Schema(dimensionNames, metricNames, schema.getColumnTypes());
  }
}
