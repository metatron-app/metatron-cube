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

package io.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.AggregateMergeRule;
import io.druid.sql.calcite.rule.DruidJoinProjectRule;
import io.druid.sql.calcite.rule.DruidJoinRule;
import io.druid.sql.calcite.rule.DruidRelToDruidRule;
import io.druid.sql.calcite.rule.DruidRules;
import io.druid.sql.calcite.rule.DruidTableScanRule;
import io.druid.sql.calcite.rule.DruidValuesRule;
import io.druid.sql.calcite.rule.ProjectAggregatePruneUnusedCallRule;
import io.druid.sql.calcite.rule.SortCollapseRule;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.MatchRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectTableScanRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

public class Rules
{
  static final Logger LOG = new Logger(DruidPlanner.class);

  public static final int DRUID_CONVENTION_RULES = 0;

  // RelOptRules.BASE_RULES
  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          TableScanRule.INSTANCE,
//          CalciteSystemProperty.COMMUTE.value() ? JoinAssociateRule.INSTANCE :
          ProjectMergeRule.INSTANCE,
          FilterTableScanRule.INSTANCE,
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          JoinPushExpressionsRule.INSTANCE,
//          AggregateExpandDistinctAggregatesRule.INSTANCE,
//          AggregateCaseToFilterRule.INSTANCE,
//          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          ProjectWindowTransposeRule.INSTANCE,
          MatchRule.INSTANCE,
//          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE,
          SortJoinTransposeRule.INSTANCE,
          SortRemoveConstantKeysRule.INSTANCE,
          SortUnionTransposeRule.INSTANCE,
          ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE,
          ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE
      );

  // Rules from CalcitePrepareImpl's createPlanner.
  private static final List<RelOptRule> MISCELLANEOUS_RULES =
      ImmutableList.of(
          Bindables.BINDABLE_TABLE_SCAN_RULE,
          ProjectTableScanRule.INSTANCE,
          ProjectTableScanRule.INTERPRETER
      );

  // RelOptRules.CONSTANT_REDUCTION_RULES
  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          ReduceExpressionsRule.PROJECT_INSTANCE,
          ReduceExpressionsRule.FILTER_INSTANCE,
          ReduceExpressionsRule.CALC_INSTANCE,
          ReduceExpressionsRule.WINDOW_INSTANCE,
          ReduceExpressionsRule.JOIN_INSTANCE,
          ValuesReduceRule.FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_FILTER_INSTANCE,
          ValuesReduceRule.PROJECT_INSTANCE,
          AggregateValuesRule.INSTANCE
      );

  // RelOptRules.ABSTRACT_RELATIONAL_RULES
  private static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES =
      ImmutableList.of(
          FilterJoinRule.FILTER_ON_JOIN,
          FilterJoinRule.JOIN,
          AbstractConverter.ExpandConversionRule.INSTANCE,
//          JoinCommuteRule.INSTANCE,
//          SemiJoinRule.PROJECT,
//          SemiJoinRule.JOIN,
          AggregateRemoveRule.INSTANCE,
          UnionToDistinctRule.INSTANCE,
          ProjectRemoveRule.INSTANCE,
          AggregateJoinTransposeRule.INSTANCE,
          AggregateMergeRule.INSTANCE,    // see ensureTypes()
          AggregateProjectMergeRule.INSTANCE,
          CalcRemoveRule.INSTANCE,
          SortRemoveRule.INSTANCE
      );

  // RelOptRules.ABSTRACT_RULES
  private static final List<RelOptRule> ABSTRACT_RULES =
      ImmutableList.of(
          AggregateProjectPullUpConstantsRule.INSTANCE2,
          UnionPullUpConstantsRule.INSTANCE,
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          UnionMergeRule.INSTANCE,
          UnionMergeRule.INTERSECT_INSTANCE,
          UnionMergeRule.MINUS_INSTANCE,
          ProjectToWindowRule.PROJECT,
          FilterMergeRule.INSTANCE,
          DateRangeRules.FILTER_INSTANCE,
          IntersectToDistinctRule.INSTANCE
      );

  private Rules()
  {
    // No instantiation.
  }

  private static final boolean DAG = false;
  private static final boolean NO_DAG = true;
  private static final int MIN_JOIN_REORDER = 6;

  public static List<Program> programs(QueryMaker queryMaker)
  {
    PlannerContext context = queryMaker.getPlannerContext();
    return ImmutableList.of(druidPrograms(context, queryMaker));
  }

  private static Program druidPrograms(PlannerContext plannerContext, QueryMaker queryMaker)
  {
    PlannerConfig config = plannerContext.getPlannerConfig();

    List<Program> programs = Lists.newArrayList();
    programs.add(Programs.subQuery(DefaultRelMetadataProvider.INSTANCE));
    programs.add(DecorrelateAndTrimFieldsProgram.INSTANCE);

    if (config.isTransitiveFilterOnjoinEnabled()) {
      programs.add(createHepProgram(
          FilterJoinRule.FILTER_ON_JOIN, FilterJoinRule.JOIN, JoinPushTransitivePredicatesRule.INSTANCE
      ));
    }
    if (config.isProjectJoinTransposeEnabled()) {
      programs.add(createHepProgram(ProjectJoinTransposeRule.INSTANCE));
    }
    if (config.isJoinReorderingEnabled()) {
      programs.add(Programs.heuristicJoinOrder(Arrays.asList(), config.isJoinReorderingBush(), MIN_JOIN_REORDER));
    }
    programs.add(Programs.ofRules(druidConventionRuleSet(plannerContext, queryMaker)));

    if (config.isJoinEnabled()) {
      // way better to be hep program in compile speed rather than be a rule in volcano
      programs.add(createHepProgram(DruidJoinProjectRule.INSTANCE));
    }

    Program program = Programs.sequence(programs.toArray(new Program[0]));
    return config.isDumpPlan() ? Dump.wrap(program) : program;
  }

  private static Program createHepProgram(RelOptRule... rules)
  {
    return createHepProgram(Arrays.asList(rules));
  }

  private static Program createHepProgram(List<RelOptRule> rules)
  {
    return createHepProgram(HepMatchOrder.TOP_DOWN, rules);
  }

  private static Program createHepProgram(HepMatchOrder order, List<RelOptRule> rules)
  {
    return Programs.of(
        new HepProgramBuilder()
            .addMatchOrder(order)
            .addRuleCollection(rules)
            .build(),
        DAG, DefaultRelMetadataProvider.INSTANCE
    );
  }

  private static Program relLogProgram(final String subject)
  {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      LOG.info("<<<< %s >>>>\n%s", subject, new Object()
      {
        @Override
        public String toString() { return RelOptUtil.toString(rel); }
      });
      return rel;
    };
  }

  private static class Delegated implements Program
  {
    final Program delegated;

    private Delegated(Program delegated) {this.delegated = delegated;}

    @Override
    public RelNode run(
        RelOptPlanner planner,
        RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices
    )
    {
      return delegated.run(planner, rel, requiredOutputTraits, materializations, lattices);
    }
  }

  private static class Dump extends Delegated
  {
    static Program wrap(Program program) { return new Dump(program);}

    private Dump(Program delegated) {super(delegated);}

    @Override
    public RelNode run(
        RelOptPlanner planner,
        RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices
    )
    {
      RelNode result = delegated.run(planner, rel, requiredOutputTraits, materializations, lattices);
      if (planner instanceof VolcanoPlanner && LOG.isInfoEnabled()) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ((VolcanoPlanner) planner).dump(pw);
        pw.flush();
        LOG.info(sw.toString());
      }
      return result;
    }
  }

  private static List<RelOptRule> druidConventionRuleSet(PlannerContext plannerContext, QueryMaker queryMaker)
  {
    List<RelOptRule> rules = Lists.newArrayList();
    rules.addAll(baseRuleSet(plannerContext));
    rules.add(new DruidTableScanRule(queryMaker));
    rules.add(new DruidValuesRule(queryMaker));
    rules.addAll(DruidRules.RULES);

    PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    if (plannerConfig.isJoinEnabled()) {
      rules.add(DruidJoinRule.instance());
    }
    if (plannerConfig.isJoinCommuteEnabled()) {
      rules.add(JoinCommuteRule.INSTANCE);
    }
    rules.add(DruidRelToDruidRule.instance());

    return ImmutableList.copyOf(rules);
  }

  private static List<RelOptRule> baseRuleSet(PlannerContext plannerContext)
  {
    final PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    final List<RelOptRule> rules = Lists.newArrayList();

    // Calcite rules.
    rules.addAll(DEFAULT_RULES);
    rules.addAll(MISCELLANEOUS_RULES);
    rules.addAll(CONSTANT_REDUCTION_RULES);
    rules.addAll(ABSTRACT_RELATIONAL_RULES);
    rules.addAll(ABSTRACT_RULES);

    if (!plannerConfig.isUseApproximateCountDistinct()) {
      // We'll need this to expand COUNT DISTINCTs.
      // Avoid AggregateExpandDistinctAggregatesRule.INSTANCE; it uses grouping sets and we don't support those.
      rules.add(AggregateExpandDistinctAggregatesRule.JOIN);
    }

    rules.add(SortCollapseRule.instance());
    rules.add(AggregateCaseToFilterRule.INSTANCE);
    rules.add(ProjectAggregatePruneUnusedCallRule.instance());

    return rules;
  }

  // Based on Calcite's Programs.DecorrelateProgram and Programs.TrimFieldsProgram, which are private and only
  // accessible through Programs.standard (which we don't want, since it also adds Enumerable rules).
  private static class DecorrelateAndTrimFieldsProgram implements Program
  {
    static final DecorrelateAndTrimFieldsProgram INSTANCE = new DecorrelateAndTrimFieldsProgram();

    @Override
    public RelNode run(
        RelOptPlanner planner,
        RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices
    )
    {
      final RelNode decorrelatedRel = RelDecorrelator.decorrelateQuery(
          rel, RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null)
      );
      final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(decorrelatedRel.getCluster(), null);
      return new RelFieldTrimmer(null, relBuilder).trim(decorrelatedRel);
    }
  }
}
