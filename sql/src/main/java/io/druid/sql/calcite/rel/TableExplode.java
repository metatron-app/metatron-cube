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

import io.druid.query.select.TableFunctionSpec;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.table.RowSignature;

import java.util.List;
import java.util.Objects;

public class TableExplode
{
  private final String opName;
  private final List<String> arguments;
  private final Filtration filtration;
  private final RowSignature outputRowSignature;

  public TableExplode(String opName, List<String> arguments, Filtration filtration, RowSignature outputRowSignature)
  {
    this.opName = opName;
    this.arguments = arguments;
    this.filtration = filtration;
    this.outputRowSignature = outputRowSignature;
  }

  public String getOpName()
  {
    return opName;
  }

  public List<String> getArguments()
  {
    return arguments;
  }

  public Filtration getFiltration()
  {
    return filtration;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public TableFunctionSpec asSpec()
  {
    return new TableFunctionSpec(arguments, filtration.getDimFilter());
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
    final TableExplode that = (TableExplode) o;
    return Objects.equals(opName, that.opName) &&
           Objects.equals(arguments, that.arguments) &&
           Objects.equals(outputRowSignature, that.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(opName, arguments, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "TableExplode{" +
           "opName=" + opName +
           "arguments=" + arguments +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
