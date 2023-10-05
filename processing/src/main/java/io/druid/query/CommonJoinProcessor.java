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
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;

import java.util.List;
import java.util.Map;

public abstract class CommonJoinProcessor extends JoinProcessor
    implements PostProcessingOperator, PostProcessingOperator.ReturnRowAs
{
  protected final JoinElement element;
  protected final boolean prefixAlias;
  protected final boolean asMap;
  protected final List<String> outputAlias;
  protected final List<String> outputColumns;   // this can be empty (count(*), for example)

  public CommonJoinProcessor(
      JoinQueryConfig config,
      JoinElement element,
      boolean prefixAlias,
      boolean asMap,
      List<String> outputAlias,
      List<String> outputColumns,
      int maxOutputRow
  )
  {
    super(config, maxOutputRow);
    this.element = element;
    this.prefixAlias = prefixAlias;
    this.asMap = asMap;
    this.outputAlias = outputAlias;
    this.outputColumns = outputColumns;
  }

  public abstract CommonJoinProcessor withAsMap(boolean asMap);

  public abstract CommonJoinProcessor withOutputColumns(List<String> outputColumns);

  @JsonProperty
  public JoinElement getElement()
  {
    return element;
  }

  @JsonProperty
  public boolean isPrefixAlias()
  {
    return prefixAlias;
  }

  @JsonProperty
  public boolean isAsMap()
  {
    return asMap;
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
    return asMap ? Map.class : Object[].class;
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

  protected int[] projection(List<String> outputAlias)
  {
    return outputColumns == null ? null : GuavaUtils.indexOf(outputAlias, outputColumns);
  }
}
