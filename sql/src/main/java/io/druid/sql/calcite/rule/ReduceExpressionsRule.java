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

import com.google.common.collect.Iterables;
import io.druid.sql.calcite.Utils;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class ReduceExpressionsRule
{
  public static final RelOptRule PROJECT_REDUCE = new ProjectReduceExpressionsRule();

  private static class ProjectReduceExpressionsRule
      extends org.apache.calcite.rel.rules.ReduceExpressionsRule.ProjectReduceExpressionsRule
  {
    public ProjectReduceExpressionsRule()
    {
      super(LogicalProject.class, true, RelFactories.LOGICAL_BUILDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      Project project = call.rel(0);
      if (!Iterables.all(project.getProjects(), Utils::isInputRef)) {
        RexBuilder builder = project.getCluster().getRexBuilder();
        RelMetadataQuery mq = call.getMetadataQuery();
        RelOptPredicateList predicates = Utils.expand(builder, mq.getPulledUpPredicates(project.getInput()));
        List<RexNode> expanded = Utils.expand(builder, project.getProjects());
        if (reduceExpressions(project, expanded, predicates) && !project.getProjects().equals(expanded)) {
          call.transformTo(
              call.builder()
                  .push(project.getInput())
                  .project(expanded, project.getRowType().getFieldNames())
                  .build());

          call.getPlanner().prune(project);
        }
      }
    }
  }
}
