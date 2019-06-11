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

import com.google.common.base.Preconditions;
import io.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Objects;

public class SortProject
{
  private final List<String> columns;
  private final RowSignature outputRowSignature;

  SortProject(
      List<String> columns,
      RowSignature outputRowSignature
  )
  {
    this.columns = Preconditions.checkNotNull(columns, "columns");
    this.outputRowSignature = Preconditions.checkNotNull(outputRowSignature, "outputRowSignature");
  }

  public List<String> getColumns()
  {
    return columns;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SortProject aggregateSortProject = (SortProject) o;
    return Objects.equals(columns, aggregateSortProject.columns) &&
           Objects.equals(outputRowSignature, aggregateSortProject.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "SortProject{" +
           "columns=" + columns +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
