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
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;

import java.util.Map;

public class RowMappingPostProcessor extends PostProcessingOperator.Abstract
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
}
