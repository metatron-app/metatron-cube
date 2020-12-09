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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.common.guava.Sequence;


import java.util.Map;

/**
 */
@JsonTypeName("toMap")
public class ToMapPostProcessor extends PostProcessingOperator.ReturnsMap
{
  private final String timestampColumn;
  private final QueryToolChestWarehouse warehouse;

  @JsonCreator
  public ToMapPostProcessor(
      @JsonProperty("timestampColumn") String timestampColumn,
      @JacksonInject QueryToolChestWarehouse warehouse
  )
  {
    this.timestampColumn = timestampColumn;
    this.warehouse = warehouse;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @Override
  public QueryRunner<Map<String, Object>> postProcess(final QueryRunner baseQueryRunner)
  {
    return new QueryRunner<Map<String, Object>>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Map<String, Object>> run(Query query, Map responseContext)
      {
        QueryToolChest toolChest = warehouse.getToolChest(query);
        Sequence sequence = baseQueryRunner.run(query, responseContext);
        return toolChest == null ? sequence : (Sequence) toolChest.asMap(query, timestampColumn).apply(sequence);
      }
    };
  }

  @Override
  public String toString()
  {
    return "ToMapPostProcessor{timestampColumn=" + timestampColumn + "}";
  }
}
