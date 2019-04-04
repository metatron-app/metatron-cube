/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import com.google.common.collect.ImmutableList;
import io.druid.common.utils.StringUtils;
import io.druid.sql.calcite.rel.DruidOuterQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.AGGREGATE;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.AGGREGATE_PROJECT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.HAVING_FILTER;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.SELECT_PROJECT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.SELECT_SORT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.SORT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.SORT_PROJECT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Stage.WHERE_FILTER;

public class DruidRules
{
  public static final Predicate<DruidRel> CAN_BUILD_ON = druidRel -> druidRel.getPartialDruidQuery() != null;

  static RelOptRuleOperand anyDruid()
  {
    return ofDruidRel(druidRel -> true);
  }

  static RelOptRuleOperand canBuildOn(PartialDruidQuery.Stage stage)
  {
    return ofDruidRel(druidRel -> druidRel.canAccept(stage));
  }

  static RelOptRuleOperand ofDruidRel(Predicate<DruidRel> predicate)
  {
    return RelOptRule.operandJ(DruidRel.class, null, predicate, RelOptRule.any());
  }

  private DruidRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return ImmutableList.of(
        DruidQueryRule.of(Filter.class, WHERE_FILTER, PartialDruidQuery::withWhereFilter),
        DruidQueryRule.of(Project.class, SELECT_PROJECT, PartialDruidQuery::withSelectProject),
        DruidQueryRule.of(Sort.class, SELECT_SORT, PartialDruidQuery::withSelectSort),
        DruidQueryRule.of(Aggregate.class, AGGREGATE, PartialDruidQuery::withAggregate),
        DruidQueryRule.of(Project.class, AGGREGATE_PROJECT, PartialDruidQuery::withAggregateProject),
        DruidQueryRule.of(Filter.class, HAVING_FILTER, PartialDruidQuery::withHavingFilter),
        DruidQueryRule.of(Sort.class, SORT, PartialDruidQuery::withSort),
        DruidQueryRule.of(Project.class, SORT_PROJECT, PartialDruidQuery::withSortProject),
        DruidOuterQueryRule.of(Filter.class, PartialDruidQuery::withWhereFilter),
        DruidOuterQueryRule.of(Project.class, PartialDruidQuery::withSelectProject),
        DruidOuterQueryRule.of(Sort.class, PartialDruidQuery::withSelectSort),
        DruidOuterQueryRule.of(Aggregate.class, PartialDruidQuery::withAggregate),
        DruidUnionRule.instance(),
        DruidSortUnionRule.instance()
    );
  }

  static class DruidQueryRule
  {
    static <RelType extends RelNode> RelOptRule of(
        final Class<RelType> relClass,
        final PartialDruidQuery.Stage stage,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f
    )
    {
      final String description = StringUtils.format("DruidQueryRule(%s)", stage);
      return new RelOptRule(RelOptRule.operand(relClass, canBuildOn(stage)), description)
      {
        @Override
        public void onMatch(final RelOptRuleCall call)
        {
          final RelType otherRel = call.rel(0);
          final DruidRel druidRel = call.rel(1);

          final PartialDruidQuery newPartialDruidQuery = f.apply(druidRel.getPartialDruidQuery(), otherRel);
          final DruidRel newDruidRel = druidRel.withPartialQuery(newPartialDruidQuery);

          if (newDruidRel.isValidDruidQuery()) {
            call.transformTo(newDruidRel);
          }
        }
      };
    }
  }

  static class DruidOuterQueryRule
  {
    static <RelType extends RelNode> RelOptRule of(
        final Class<RelType> relClass,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f
    )
    {
      final String description = StringUtils.format("DruidOuterQueryRule(%s)", relClass.getSimpleName());
      return new RelOptRule(RelOptRule.operand(relClass, anyDruid()), description)
      {
        @Override
        public void onMatch(final RelOptRuleCall call)
        {
          final RelType otherRel = call.rel(0);
          final DruidRel druidRel = call.rel(1);

          final RelNode leafRel = druidRel.getLeafRel();
          final PartialDruidQuery newPartialDruidQuery = f.apply(PartialDruidQuery.create(leafRel), otherRel);
          final DruidRel newDruidRel = DruidOuterQueryRel.create(druidRel, newPartialDruidQuery);

          if (newDruidRel.isValidDruidQuery()) {
            call.transformTo(newDruidRel);
          }
        }
      };
    }
  }
}
