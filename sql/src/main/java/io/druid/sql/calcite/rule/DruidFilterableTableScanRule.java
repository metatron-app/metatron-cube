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

import com.google.common.collect.Lists;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidValuesRel;
import io.druid.sql.calcite.rel.QueryMaker;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

public class DruidFilterableTableScanRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidFilterableTableScanRule(final QueryMaker queryMaker)
  {
    super(
        DruidRel.operand(
            LogicalFilter.class,
            DruidRel.operand(
                LogicalTableScan.class,
                scan -> scan.getTable().unwrap(FilterableTable.class) != null
            )
        )
    );
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final LogicalFilter filter = call.rel(0);
    final LogicalTableScan scan = call.rel(1);

    final RelOptTable table = scan.getTable();
    final List<RexNode> filters = Lists.newArrayList(Utils.decomposeOnAnd(filter.getCondition()));
    final DruidValuesRel valuesRel = DruidValuesRel.of(
        scan, filter, table.unwrap(FilterableTable.class).scan(null, filters), queryMaker
    );
    final RelBuilder relBuilder = call.builder().push(valuesRel);
    if (!filters.isEmpty()) {
      relBuilder.filter(filters.toArray(new RexNode[0]));
    }
    call.transformTo(relBuilder.build());
  }
}
