/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import com.google.common.collect.ImmutableList;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Objects;

public class Windowing
{
  private final List<String> partitionColumns;
  private final List<OrderByColumnSpec> sortCoumns;
  private final List<String> expression;

  private final RowSignature outputRowSignature;

  public Windowing(
      final List<String> partitionColumns,
      final List<OrderByColumnSpec> sortCoumns,
      final List<String> expression,
      final RowSignature outputRowSignature
  )
  {
    this.partitionColumns = ImmutableList.copyOf(partitionColumns);
    this.sortCoumns = ImmutableList.copyOf(sortCoumns);
    this.expression = ImmutableList.copyOf(expression);
    this.outputRowSignature = outputRowSignature;
  }

  public WindowingSpec asSpec()
  {
    return new WindowingSpec(partitionColumns, sortCoumns, expression, null, null);
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Windowing grouping = (Windowing) o;
    return Objects.equals(partitionColumns, grouping.partitionColumns) &&
           Objects.equals(sortCoumns, grouping.sortCoumns) &&
           Objects.equals(expression, grouping.expression) &&
           Objects.equals(outputRowSignature, grouping.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionColumns, sortCoumns, expression, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "Windowing{" +
           "partitionColumns=" + partitionColumns +
           ", sortCoumns=" + sortCoumns +
           ", expression=" + expression +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
