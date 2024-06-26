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

import com.google.common.collect.ImmutableMap;
import io.druid.sql.calcite.planner.PlannerFactory;

import java.util.Map;

/**
 * View manager that does not support views.
 */
public class NoopViewManager implements ViewManager
{
  @Override
  public void createView(final PlannerFactory plannerFactory, final String viewName, final String viewSql)
  {
    throw new UnsupportedOperationException("Noop view manager cannot do anything");
  }

  @Override
  public void alterView(final PlannerFactory plannerFactory, final String viewName, final String viewSql)
  {
    throw new UnsupportedOperationException("Noop view manager cannot do anything");
  }

  @Override
  public void dropView(final String viewName)
  {
    throw new UnsupportedOperationException("Noop view manager cannot do anything");
  }

  @Override
  public Map<String, DruidViewMacro> getViews()
  {
    return ImmutableMap.of();
  }
}
