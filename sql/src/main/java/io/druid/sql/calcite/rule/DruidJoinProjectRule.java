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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;
import java.util.function.Predicate;

public class DruidJoinProjectRule extends RelOptRule
{
  private static final Predicate<DruidRel> HAS_PROJECTION = druidRel ->
      druidRel.getPartialDruidQuery().getScanProject() != null;

  // todo: can merge projects
  private static final Predicate<DruidRel> HAS_NO_PROJECTION = druidRel ->
      ((DruidJoinRel) druidRel).getOutputColumns() == null;

  public static final DruidJoinProjectRule INSTANCE = new DruidJoinProjectRule();

  private DruidJoinProjectRule()
  {
    super(DruidRel.of(DruidOuterQueryRel.class, HAS_PROJECTION, DruidRel.of(DruidJoinRel.class, HAS_NO_PROJECTION)));
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
    final int[] indices = Utils.collectInputRefs(childExps);

    DruidRel newJoin = join.withOutputColumns(ImmutableIntList.of(indices));

    if (!Utils.isAllInputRef(childExps)) {
      List<RexNode> rewritten = Utils.rewrite(join.getCluster().getRexBuilder(), childExps, indices);
      Project newProject = LogicalProject.create(newJoin, rewritten, project.getRowType());
      newJoin = DruidOuterQueryRel.create(newJoin, druidQuery.withScanProject(newJoin, newProject));
    } else if (!druidQuery.isProjectOnly()) {
      newJoin = DruidOuterQueryRel.create(newJoin, druidQuery.withScanProject(newJoin, null));
    }
    call.transformTo(newJoin);
  }
}
