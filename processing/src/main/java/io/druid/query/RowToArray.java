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
import com.google.common.base.Preconditions;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;

import java.util.List;
import java.util.Map;

/**
 */
public class RowToArray extends PostProcessingOperator.Abstract<Row>
{
  private final List<String> columnNames;

  @JsonCreator
  public RowToArray(@JsonProperty("columnNames") List<String> columnNames)
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(columnNames), "'columnNames' should not be null or empty");
    this.columnNames = columnNames;
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner<Row> baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        return Sequences.map(
            baseRunner.run(query, responseContext),
            Rows.rowToArray(columnNames.toArray(new String[0]))
        );
      }
    };
  }

  @Override
  public String toString()
  {
    return "RowToArray{" +
           "columnNames=" + columnNames +
           '}';
  }
}
