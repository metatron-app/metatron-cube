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

import com.google.common.collect.ImmutableList;
import io.druid.common.utils.StringUtils;
import io.druid.sql.calcite.rel.DruidOuterQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.DruidUnionRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.AGGREGATE;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.FILTER;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.PROJECT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.SORT;
import static io.druid.sql.calcite.rel.PartialDruidQuery.Operator.WINDOW;

public class DruidRules
{
  public static final List<RelOptRule> RULES = ImmutableList.of(
      DruidRule.of(Filter.class, FILTER, PartialDruidQuery::withFilter),
      DruidRule.of(Project.class, PROJECT, PartialDruidQuery::withProject),
      DruidRule.of(Aggregate.class, AGGREGATE, PartialDruidQuery::withAggregate),
      DruidRule.of(Window.class, WINDOW, PartialDruidQuery::withWindow),
      DruidRule.of(Sort.class, SORT, PartialDruidQuery::withSort),
      DruidUnionRule.INSTANCE,
      DruidSortUnionRule.INSTANCE
  );

  static RelOptRuleOperand anyDruid()
  {
    return DruidRel.of(DruidRel.class, druidRel -> true);
  }

  private DruidRules()
  {
    // No instantiation.
  }

  private static class DruidRule
  {
    private static <RelType extends RelNode> RelOptRule of(
        final Class<RelType> relClass,
        final PartialDruidQuery.Operator operator,
        final BiFunction<PartialDruidQuery, RelType, PartialDruidQuery> f
    )
    {
      return new RelOptRule(RelOptRule.operand(relClass, anyDruid()), StringUtils.format("DruidRule(%s)", operator))
      {
        @Override
        public void onMatch(final RelOptRuleCall call)
        {
          final RelType otherRel = call.rel(0);
          final DruidRel druidRel = call.rel(1);

          RelNode newDruidRel = tryMergeRel(otherRel, druidRel);
          if (newDruidRel == null) {
            newDruidRel = tryOuterRel(otherRel, druidRel);
          }
          if (newDruidRel != null) {
            call.transformTo(newDruidRel);
          }
        }

        private RelNode tryMergeRel(RelType otherRel, DruidRel druidRel)
        {
          final PartialDruidQuery druidQuery = druidRel.getPartialDruidQuery();
          if (druidQuery != null) {
            final PartialDruidQuery newDruidQuery = f.apply(druidQuery, otherRel);
            if (newDruidQuery != null) {
              if (newDruidQuery.isScanOnly()) {
                return newDruidQuery.getScan();   // prevents circular reference
              }
              return druidRel.withPartialQuery(newDruidQuery);
            }
          }
          return null;
        }

        private RelNode tryOuterRel(RelType otherRel, DruidRel druidRel)
        {
          final PartialDruidQuery newDruidQuery = f.apply(PartialDruidQuery.create(druidRel), otherRel);
          if (newDruidQuery != null) {
            return DruidOuterQueryRel.create(druidRel, newDruidQuery);
          }
          return null;
        }
      };
    }
  }

  private static class DruidUnionRule extends RelOptRule
  {
    private static final DruidUnionRule INSTANCE = new DruidUnionRule();

    private DruidUnionRule()
    {
      super(operand(Union.class, unordered(operand(DruidRel.class, any()))));
    }

    @Override
    public void onMatch(final RelOptRuleCall call)
    {
      final Union unionRel = call.rel(0);
      final DruidRel someDruidRel = call.rel(1);
      final List<RelNode> inputs = unionRel.getInputs();

      if (unionRel.all) {
        // Can only do UNION ALL.
        call.transformTo(DruidUnionRel.create(
            someDruidRel.getQueryMaker(),
            unionRel.getRowType(),
            inputs,
            -1
        ));
      }
    }
  }

  private static class DruidSortUnionRule extends RelOptRule
  {
    private static final DruidSortUnionRule INSTANCE = new DruidSortUnionRule();

    private DruidSortUnionRule()
    {
      super(operand(Sort.class, operand(DruidUnionRel.class, any())));
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      // LIMIT, no ORDER BY
      final Sort sort = call.rel(0);
      return sort.collation.getFieldCollations().isEmpty() && sort.fetch != null;
    }

    @Override
    public void onMatch(final RelOptRuleCall call)
    {
      final Sort sort = call.rel(0);
      final DruidUnionRel unionRel = call.rel(1);

      final int limit = RexLiteral.intValue(sort.fetch);
      final int offset = sort.offset != null ? RexLiteral.intValue(sort.offset) : 0;

      final DruidUnionRel newUnionRel = DruidUnionRel.create(
          unionRel.getQueryMaker(),
          unionRel.getRowType(),
          unionRel.getInputs(),
          unionRel.getLimit() >= 0 ? Math.min(limit + offset, unionRel.getLimit()) : limit + offset
      );

      if (offset == 0) {
        call.transformTo(newUnionRel);
      } else {
        call.transformTo(
            call.builder()
                .push(newUnionRel)
                .sortLimit(offset, -1, Collections.emptyList())
                .build()
        );
      }
    }
  }
}
