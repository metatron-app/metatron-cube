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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.DateTimes;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class ArrayToRow extends PostProcessingOperator.Abstract<Object[]>
{
  private final List<String> columnNames;

  @JsonCreator
  public ArrayToRow(@JsonProperty("columnNames") List<String> columnNames)
  {
    this.columnNames = columnNames;
    Preconditions.checkArgument(columnNames.contains(Row.TIME_COLUMN_NAME));
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner<Object[]> baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final int timeIndex = columnNames.indexOf(Row.TIME_COLUMN_NAME);
        return Sequences.map(baseRunner.run(query, responseContext), new Function<Object[], Row>()
        {
          @Override
          public Row apply(Object[] input)
          {
            DateTime datetime = null;
            final int limit = Math.min(input.length, columnNames.size());
            final Map<String, Object> map = Maps.newHashMapWithExpectedSize(limit);
            for (int i = 0; i < limit; i++) {
              if (i == timeIndex) {
                datetime = DateTimes.utc(((Number) input[i]).longValue());
              } else {
                map.put(columnNames.get(i), input[i]);
              }
            }
            return new MapBasedRow(datetime, map);
          }
        });
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
