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
import com.metamx.common.logger.Logger;
import io.druid.common.utils.StringUtils;
import io.druid.sql.calcite.rel.DruidOuterQueryRel;
import io.druid.sql.calcite.rel.DruidQueryRel;
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
import org.apache.calcite.rel.core.Window;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.AGGREGATE;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.FILTER;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.PROJECT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.SORT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.WINDOW;

public class DruidRules
{
  private static final Logger LOG = new Logger(DruidRules.class);

  static RelOptRuleOperand anyDruid()
  {
    return ofDruidRel(druidRel -> true);
  }

  static RelOptRuleOperand canBuildOn(PartialDruidQuery.Operator operator)
  {
    return ofDruidRel(druidRel -> druidRel.canAccept(operator));
  }

  static RelOptRuleOperand ofDruidRel(Predicate<DruidRel> predicate)
  {
    return ofDruidRel(DruidRel.class, predicate);
  }

  static RelOptRuleOperand ofDruidQueryRel(Predicate<DruidRel> predicate)
  {
    return ofDruidRel(DruidQueryRel.class, predicate);
  }

  static <T extends DruidRel> RelOptRuleOperand ofDruidRel(Class<T> relClass, Predicate<DruidRel> predicate)
  {
    return RelOptRule.operandJ(relClass, null, predicate, RelOptRule.any());
  }

  private DruidRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return ImmutableList.of(
        DruidQueryRule.of(Filter.class, FILTER, PartialDruidQuery::withFilter),
        DruidQueryRule.of(Project.class, PROJECT, PartialDruidQuery::withProject),
        DruidQueryRule.of(Aggregate.class, AGGREGATE, PartialDruidQuery::withAggregate),
        DruidQueryRule.of(Window.class, WINDOW, PartialDruidQuery::withWindow),
        DruidQueryRule.of(Sort.class, SORT, PartialDruidQuery::withSort),
        DruidOuterQueryRule.of(Filter.class, PartialDruidQuery::withFilter),
        DruidOuterQueryRule.of(Project.class, PartialDruidQuery::withProject),
        DruidOuterQueryRule.of(Aggregate.class, PartialDruidQuery::withAggregate),
        DruidOuterQueryRule.of(Window.class, PartialDruidQuery::withWindow),
        DruidOuterQueryRule.of(Sort.class, PartialDruidQuery::withSort),
        DruidUnionRule.instance(),
        DruidSortUnionRule.instance()
    );
  }

  static class DruidQueryRule
  {
    static <RelType extends RelNode> RelOptRule of(
        final Class<RelType> relClass,
        final PartialDruidQuery.Operator operator,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f
    )
    {
      final String description = StringUtils.format("DruidQueryRule(%s)", operator);
      return new RelOptRule(RelOptRule.operand(relClass, canBuildOn(operator)), description)
      {
        @Override
        public void onMatch(final RelOptRuleCall call)
        {
          final RelType otherRel = call.rel(0);
          final DruidRel druidRel = call.rel(1);

          final PartialDruidQuery druidQuery = druidRel.getPartialDruidQuery();
          final PartialDruidQuery newDruidQuery = f.apply(druidQuery, otherRel);
          if (newDruidQuery == null) {
//            LOG.info(" %s + %s ---> x", druidQuery.stage(), operator);
            return;   // quick check
          }
          final DruidRel newDruidRel = druidRel.withPartialQuery(newDruidQuery);

          if (newDruidRel.isValidDruidQuery()) {
//            LOG.info(" %s + %s ---> %s", druidQuery.stage(), operator, newDruidQuery.stage());
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
          if (newPartialDruidQuery == null) {
            return;
          }
          final DruidRel newDruidRel = DruidOuterQueryRel.create(druidRel, newPartialDruidQuery);

          if (newDruidRel.isValidDruidQuery()) {
            call.transformTo(newDruidRel);
          }
        }
      };
    }
  }
}
