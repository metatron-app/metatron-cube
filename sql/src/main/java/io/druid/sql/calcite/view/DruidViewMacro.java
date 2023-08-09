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

package io.druid.sql.calcite.view;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Escalator;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.schema.DruidSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;

import java.util.List;

public class DruidViewMacro implements TableMacro
{
  private final PlannerFactory plannerFactory;
  private final Escalator escalator;
  private final String viewSql;

  @Inject
  public DruidViewMacro(
      final PlannerFactory plannerFactory,
      final Escalator escalator,
      final String viewSql
  )
  {
    this.plannerFactory = plannerFactory;
    this.escalator = escalator;
    this.viewSql = viewSql;
  }

  @Override
  public TranslatableTable apply(final List<?> arguments)
  {
    final RelDataType rowType;
    // Using an escalator here is a hack, but it's currently needed to get the row type. Ideally, some
    // later refactoring would make this unnecessary, since there is no actual query going out herem.
    final AuthenticationResult authenticationResult = escalator.createEscalatedAuthenticationResult();
    try (final DruidPlanner planner = plannerFactory.createPlanner(null, authenticationResult)) {
      rowType = planner.plan(viewSql, null).rowType();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new ViewTable(
        null,
        RelDataTypeImpl.proto(rowType),
        viewSql,
        ImmutableList.of(DruidSchema.NAME),
        null
    );
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return ImmutableList.of();
  }
}
