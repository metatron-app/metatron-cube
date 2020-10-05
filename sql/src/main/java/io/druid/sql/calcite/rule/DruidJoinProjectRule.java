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

import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidJoinRel;
import io.druid.sql.calcite.rel.DruidOuterQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;
import java.util.function.Predicate;

public class DruidJoinProjectRule extends RelOptRule
{
  private static final Predicate<DruidRel> HAS_PROJECTION = druidRel ->
      druidRel.getPartialDruidQuery().getScanProject() != null;

  public static final DruidJoinProjectRule INSTANCE = new DruidJoinProjectRule();

  private DruidJoinProjectRule()
  {
    super(DruidRules.ofDruidRel(DruidOuterQueryRel.class, HAS_PROJECTION, operand(DruidJoinRel.class, any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final DruidOuterQueryRel outer = call.rel(0);
    final DruidJoinRel join = call.rel(1);
    final PartialDruidQuery druidQuery = outer.getPartialDruidQuery();
    if (druidQuery.getScanFilter() != null) {
      return;   // todo
    }
    final Project project = druidQuery.getScanProject();
    final List<RexNode> childExps = project.getChildExps();
    if (!Utils.isAllInputRef(childExps)) {
      return;
    }
    final int[] outputColumns = new int[childExps.size()];
    for (int i = 0; i < outputColumns.length; i++) {
      outputColumns[i] = ((RexInputRef) childExps.get(i)).getIndex();
    }
    DruidRel converted = join.withOutputColumns(ImmutableIntList.of(outputColumns));
    if (!druidQuery.isProjectOnly()) {
      converted = DruidOuterQueryRel.create(converted, druidQuery.withoutScanProject(converted));
    }
    call.transformTo(converted);
  }
}
