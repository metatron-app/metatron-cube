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

package io.druid.sql.calcite.planner;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.Sequence;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class PlannerResult
{
  private final Function<List<Object>, Sequence<Object[]>> function;
  private final RelDataType rowType;
  private final List<RelDataType> parametersType;
  private final Set<String> datasourceNames;
  private final AtomicBoolean didRun = new AtomicBoolean();

  public PlannerResult(Function<List<Object>, Sequence<Object[]>> function, RelDataType rowType)
  {
    this(function, rowType, null, ImmutableSet.of());
  }

  public PlannerResult(
      final Function<List<Object>, Sequence<Object[]>> function,
      final RelDataType rowType,
      final List<RelDataType> parametersType,
      final Set<String> datasourceNames
  )
  {
    this.function = function;
    this.rowType = rowType;
    this.parametersType = parametersType;
    this.datasourceNames = ImmutableSet.copyOf(datasourceNames);
  }

  public Sequence<Object[]> run(List<Object> parameters)
  {
    if (!didRun.compareAndSet(false, true)) {
      // Safety check.
      throw new IllegalStateException("Cannot run more than once");
    }
    return function.apply(parameters);
  }

  public RelDataType rowType()
  {
    return rowType;
  }

  public List<RelDataType> parametersType()
  {
    return parametersType == null ? ImmutableList.of() : parametersType;
  }

  public Set<String> datasourceNames()
  {
    return datasourceNames;
  }
}
