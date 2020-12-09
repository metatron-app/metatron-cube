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
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.guava.Sequence;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("arrayToRow")
public class ArrayToRow extends PostProcessingOperator.ReturnsRow<Object[]>
{
  private final List<String> columnNames;
  private final String timeColumnName;

  @JsonCreator
  public ArrayToRow(
      @JsonProperty("columnNames") List<String> columnNames,
      @JsonProperty("timeColumnName") String timeColumnName
  )
  {
    this.columnNames = columnNames;
    this.timeColumnName = timeColumnName;
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeColumnName()
  {
    return timeColumnName;
  }

  @Override
  public QueryRunner<Row> postProcess(final QueryRunner<Object[]> baseRunner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<Row> run(Query query, Map responseContext)
      {
        return Sequences.map(
            baseRunner.run(query, responseContext),
            arrayToRow(columnNames, timeColumnName == null ? Row.TIME_COLUMN_NAME : timeColumnName));
      }
    };
  }

  public static Function<Object[], Row> arrayToRow(final List<String> columnNames)
  {
    return arrayToRow(columnNames, Row.TIME_COLUMN_NAME);
  }

  public static Function<Object[], Row> arrayToRow(final List<String> columnNames, final String timeColumn)
  {
    return new Function<Object[], Row>()
    {
      private final int timeIndex = columnNames.indexOf(timeColumn);

      @Override
      public Row apply(final Object[] input)
      {
        long datetime = 0;
        final int limit = Math.min(input.length, columnNames.size());
        final Map<String, Object> map = Maps.newHashMapWithExpectedSize(limit);
        for (int i = 0; i < limit; i++) {
          if (i == timeIndex && input[i] != null) {
            datetime = ((Number) input[i]).longValue();
          } else {
            map.put(columnNames.get(i), input[i]);
          }
        }
        return new MapBasedRow(datetime, map);
      }
    };
  }

  @Override
  public String toString()
  {
    return "ArrayToRow{" +
           "columnNames=" + columnNames +
           '}';
  }
}
