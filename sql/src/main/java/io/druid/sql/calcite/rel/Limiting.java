/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.rel;

import com.google.common.base.Suppliers;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Map;

public class Limiting
{
  private final List<WindowingSpec> windowingSpecs;
  private final List<OrderByColumnSpec> columns;
  private final int limit;
  private final Map<String, String> alias;
  private final RowSignature inputRowSignature;
  private final RowSignature outputRowSignature;

  public Limiting(
      List<WindowingSpec> windowingSpecs,
      List<OrderByColumnSpec> columns,
      Integer limit,
      Map<String, String> alias,
      RowSignature inputRowSignature,
      RowSignature outputRowSignature
  )
  {
    this.windowingSpecs = windowingSpecs;
    this.columns = columns;
    this.limit = limit != null ? limit : -1;
    this.alias = alias;
    this.inputRowSignature = inputRowSignature;
    this.outputRowSignature = outputRowSignature;
  }

  public List<OrderByColumnSpec> getColumns()
  {
    return columns;
  }

  public int getLimit()
  {
    return limit;
  }

  public RowSignature getInputRowSignature()
  {
    return inputRowSignature;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public LimitSpec getLimitSpec()
  {
    return new LimitSpec(columns, limit, null, null, windowingSpecs, alias, Suppliers.ofInstance(inputRowSignature));
  }
}
