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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.groupby.orderby.LimitSpec;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class CommonJoinProcessor extends JoinProcessor
    implements PostProcessingOperator, PostProcessingOperator.ReturnRowAs
{
  protected final boolean prefixAlias;
  protected final boolean asArray;
  protected final List<String> outputColumns;
  protected final int maxOutputRow;

  public CommonJoinProcessor(
      JoinQueryConfig config,
      boolean prefixAlias,
      boolean asArray,
      List<String> outputColumns,
      int maxOutputRow
  )
  {
    super(config, maxOutputRow);
    this.prefixAlias = prefixAlias;
    this.asArray = asArray;
    this.outputColumns = outputColumns;
    this.maxOutputRow = config == null ? maxOutputRow : config.getMaxOutputRow(maxOutputRow);
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

  protected Sequence projection(Iterator<Object[]> outputRows, List<String> columns)
  {
    if (asArray) {
      return Sequences.once(GuavaUtils.map(outputRows, LimitSpec.remap(columns, outputColumns)));
    }
    return Sequences.once(GuavaUtils.map(outputRows, toMap(columns, outputColumns)));
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
