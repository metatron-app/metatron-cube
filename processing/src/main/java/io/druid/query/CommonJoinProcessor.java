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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.CompactRow;
import io.druid.data.input.Rows;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.segment.column.Column;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class CommonJoinProcessor extends JoinProcessor
    implements PostProcessingOperator, PostProcessingOperator.ReturnRowAs
{
  protected final boolean prefixAlias;
  protected final boolean asArray;
  protected final List<String> outputAlias;
  protected final List<String> outputColumns;

  public CommonJoinProcessor(
      JoinQueryConfig config,
      boolean prefixAlias,
      boolean asArray,
      List<String> outputAlias,
      List<String> outputColumns,
      int maxOutputRow
  )
  {
    super(config, maxOutputRow);
    this.prefixAlias = prefixAlias;
    this.asArray = asArray;
    this.outputAlias = outputAlias;
    this.outputColumns = outputColumns;
  }

  public abstract CommonJoinProcessor withAsArray(boolean asArray);

  @JsonProperty
  public boolean isPrefixAlias()
  {
    return prefixAlias;
  }

  @JsonProperty
  public boolean isAsArray()
  {
    return asArray;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getOutputAlias()
  {
    return outputAlias;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @JsonProperty
  public int getMaxOutputRow()
  {
    return maxOutputRow;
  }

  @Override
  public Class rowClass()
  {
    return asArray ? Object[].class : Map.class;
  }

  protected List<String> concatColumnNames(List<List<String>> columnsList, List<String> aliases)
  {
    final List<String> outputColumns = Lists.newArrayList();
    for (int i = 0; i < columnsList.size(); i++) {
      List<String> columns = columnsList.get(i);
      if (aliases == null) {
        outputColumns.addAll(columns);
      } else {
        String alias = aliases.get(i) + ".";
        for (String column : columns) {
          outputColumns.add(alias + column);
        }
      }
    }
    return outputColumns;
  }

  @SuppressWarnings("unchecked")
  protected Sequence projection(Iterator<Object[]> outputRows, List<String> outputAlias, boolean asRow)
  {
    final List<String> projectedNames = outputColumns == null ? outputAlias : outputColumns;
    if (asArray) {
      Iterator iterator = GuavaUtils.map(outputRows, LimitSpec.remap(outputAlias, projectedNames));
      return Sequences.once(projectedNames, asRow ? GuavaUtils.map(iterator, CompactRow.WRAP) : iterator);
    }
    Iterator iterator = GuavaUtils.map(outputRows, toMap(outputAlias, projectedNames));
    return Sequences.once(projectedNames, asRow ? GuavaUtils.map(iterator, Rows.mapToRow(Column.TIME_COLUMN_NAME)) : iterator);
  }

  private static Function<Object[], Map<String, Object>> toMap(List<String> inputColumns, List<String> outputColumns)
  {
    if (!GuavaUtils.isNullOrEmpty(outputColumns)) {
      final int[] indices = GuavaUtils.indexOf(inputColumns, outputColumns);
      return new Function<Object[], Map<String, Object>>()
      {
        @Override
        public Map<String, Object> apply(final Object[] input)
        {
          final Map<String, Object> event = Maps.newLinkedHashMap();
          for (int i = 0; i < indices.length; i++) {
            if (indices[i] >= 0) {
              event.put(outputColumns.get(i), input[indices[i]]);
            }
          }
          return event;
        }
      };
    }
    return new Function<Object[], Map<String, Object>>()
    {
      @Override
      public Map<String, Object> apply(Object[] input)
      {
        final Map<String, Object> event = Maps.newLinkedHashMap();
        for (int i = 0; i < input.length; i++) {
          event.put(inputColumns.get(i), input[i]);
        }
        return event;
      }
    };
  }
}
