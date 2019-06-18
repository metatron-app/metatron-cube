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
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;

import java.util.Map;

/**
 */
@SuppressWarnings("unchecked")
public class RowToMap extends PostProcessingOperator.Abstract<Row>
{
  private final String timestampColumn;

  @JsonCreator
  public RowToMap(@JsonProperty("timestampColumn") String timestampColumn)
  {
    this.timestampColumn = timestampColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner<Row> baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        return Sequences.map(baseRunner.run(query, responseContext), Rows.rowToMap(timestampColumn));
      }
    };
  }

  @Override
  public boolean hasTabularOutput()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "RowToMap{" +
           "timestampColumn=" + timestampColumn +
           '}';
  }
}
