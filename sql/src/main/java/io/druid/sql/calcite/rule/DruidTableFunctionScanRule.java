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

package io.druid.sql.calcite.rule;

import io.druid.sql.calcite.rel.DruidTableFunctionScanRel;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;

public class DruidTableFunctionScanRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidTableFunctionScanRule(final QueryMaker queryMaker)
  {
    super(operand(LogicalTableFunctionScan.class, any()));
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    call.transformTo(DruidTableFunctionScanRel.of(call.rel(0), queryMaker));
  }
}
