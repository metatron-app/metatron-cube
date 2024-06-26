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

import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidValuesRel;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.ScannableTable;

public class DruidTableScanRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidTableScanRule(final QueryMaker queryMaker)
  {
    super(DruidRel.operand(LogicalTableScan.class));
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final LogicalTableScan scan = call.rel(0);
    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);
    if (druidTable != null) {
      call.transformTo(
          DruidQueryRel.fullScan(scan, table, druidTable, queryMaker)
      );
    } else {
      final ScannableTable scannable = table.unwrap(ScannableTable.class);
      if (scannable != null) {
        call.transformTo(
            DruidValuesRel.of(scan, scannable.scan(null), queryMaker)
        );
      }
    }
  }
}
