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

import com.google.common.collect.ImmutableSet;
import io.druid.collections.IntList;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidJoinRel;
import io.druid.sql.calcite.rel.DruidOuterQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.util.ImmutableIntList;

import java.util.Arrays;
import java.util.List;

public class DruidProjectPushdownRule
{
  public static final ProjectToJoinRule PROJECT_TO_JOIN = new ProjectToJoinRule();

  private static class ProjectToJoinRule extends RelOptRule
  {
    private ProjectToJoinRule()
    {
      super(
          DruidRel.operand(DruidOuterQueryRel.class, r -> r.getPartialDruidQuery().getScanProject() != null,
                           DruidRel.operand(DruidJoinRel.class, r -> r.getOutputColumns() == null)
          )
      );
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final DruidOuterQueryRel outer = call.rel(0);
      final DruidJoinRel join = call.rel(1);
      final PartialDruidQuery druidQuery = outer.getPartialDruidQuery();

      final Filter filter = druidQuery.getScanFilter();
      final Project project = druidQuery.getScanProject();

      // from ProjectFilterTransposeRule
      if (RexOver.containsOver(project.getProjects(), null)) {
        return;
      }
      if (project.getRowType().isStruct() &&
          project.getRowType().getFieldList().stream().anyMatch(RelDataTypeField::isDynamicStar)) {
        return;
      }

      final List<RexNode> childExps = project.getProjects();
      final IntList indices = Utils.collectInputRefs(childExps);
      final int projectLen = indices.size();
      if (filter != null) {
        for (int x : Utils.collectInputRefs(Arrays.asList(filter.getCondition()))) {
          if (indices.indexOf(x) < 0) {
            indices.add(x);
          }
        }
      }
      final int[] mapping = indices.array();
      final RexBuilder builder = join.getCluster().getRexBuilder();
      final DruidRel newJoin = join.withOutputColumns(ImmutableIntList.of(mapping));

      final int[] revert = Utils.revert(mapping);

      Project newProject = null;
      if (projectLen != mapping.length || !Utils.isAllInputRefs(childExps)) {
        List<RexNode> rewritten = Utils.rewrite(builder, childExps, revert);
        newProject = LogicalProject.create(
            newJoin,
            Arrays.asList(),
            rewritten,
            project.getRowType(),
            ImmutableSet.of()
        );
      }
      Filter newFilter = null;
      if (filter != null) {
        RexNode rewritten = Utils.rewrite(builder, filter.getCondition(), revert);
        newFilter = LogicalFilter.create(newJoin, rewritten);
      }
      PartialDruidQuery rewritten = druidQuery.withScanProject(newJoin, newFilter, newProject);
      call.transformTo(rewritten.isScanOnly() ? newJoin : DruidOuterQueryRel.create(newJoin, rewritten));
    }
  }

  public static final JoinToJoinRule JOIN_TO_JOIN_LEFT = new JoinToJoinRule(
      DruidRel.operand(DruidJoinRel.class, r -> r.getOutputColumns() == null), DruidRel.anyDruid()
  );
  public static final JoinToJoinRule JOIN_TO_JOIN_RIGHT = new JoinToJoinRule(
      DruidRel.anyDruid(), DruidRel.operand(DruidJoinRel.class, r -> r.getOutputColumns() == null)
  );

  private static class JoinToJoinRule extends RelOptRule
  {
    private JoinToJoinRule(RelOptRuleOperand operand0, RelOptRuleOperand... operands)
    {
      super(DruidRel.operand(DruidJoinRel.class, r -> r.getOutputColumns() != null, operand0, operands));
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      DruidJoinRel join = call.rel(0);
      DruidRel left = call.rel(1);
      DruidRel right = call.rel(2);
      if (left instanceof DruidJoinRel) {
        call.transformTo(join.rewrite((DruidJoinRel) left, true));
      } else if (right instanceof DruidJoinRel) {
        call.transformTo(join.rewrite((DruidJoinRel) right, false));
      }
    }
  }
}