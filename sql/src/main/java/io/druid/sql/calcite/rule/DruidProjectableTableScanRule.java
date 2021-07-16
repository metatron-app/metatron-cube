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

import io.druid.collections.IntList;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidValuesRel;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.ProjectableFilterableTable;

public class DruidProjectableTableScanRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidProjectableTableScanRule(final QueryMaker queryMaker)
  {
    super(
        operandJ(
            LogicalProject.class,
            null,
            p -> Utils.isAllInputRef(p.getProjects()),
            some(
                DruidRel.of(
                    LogicalTableScan.class,
                    scan -> scan.getTable().unwrap(ProjectableFilterableTable.class) != null
                )
            )
        )
    );
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final LogicalProject project = call.rel(0);
    final LogicalTableScan scan = call.rel(1);

    final RelOptTable table = scan.getTable();
    final IntList inputRefs = Utils.collectInputRefs(project.getProjects());

    final Enumerable<Object[]> projected = table.unwrap(ProjectableFilterableTable.class)
                                                .scan(null, null, inputRefs.array());

    call.transformTo(DruidValuesRel.of(scan, project, projected, queryMaker));
  }
}
