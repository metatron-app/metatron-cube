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
import io.druid.common.guava.DSuppliers;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.DruidFilterableTableScanRule;
import io.druid.sql.calcite.rule.DruidJoinProjectRule;
import io.druid.sql.calcite.rule.DruidProjectableTableScanRule;
import io.druid.sql.calcite.rule.DruidRelToDruidRule;
import io.druid.sql.calcite.rule.DruidRules;
import io.druid.sql.calcite.rule.DruidTableFunctionScanRule;
import io.druid.sql.calcite.rule.DruidTableScanRule;
import io.druid.sql.calcite.rule.DruidValuesRule;
import io.druid.sql.calcite.rule.PreFilteringRule;
import io.druid.sql.calcite.rule.ProjectAggregatePruneUnusedCallRule;
import io.druid.sql.calcite.rule.ReduceExpressionsRule;
import io.druid.sql.calcite.rule.SortCollapseRule;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

public class Rules
{
  static final Logger LOG = new Logger(DruidPlanner.class);

  public static final int DRUID_CONVENTION_RULES = 0;

  // Based on Calcite's Programs.DecorrelateProgram and Programs.TrimFieldsProgram, which are private and only
  // accessible through Programs.standard (which we don't want, since it also adds Enumerable rules).
  static final Program DECORRELATE_TRIM_PROGRAM = (planner, rel, traits, materializations, lattices) -> {
    final RelBuilder builder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
    return new RelFieldTrimmer(null, builder).trim(RelDecorrelator.decorrelateQuery(rel, builder));
  };

  // set collation on distributed aggregation operator
  static final RelOptRule SET_COLLATION_ON_AGGREGATE = new RelOptRule(
      DruidRel.operand(Aggregate.class, Utils::distributed), "SET_COLLATION_ON_AGGREGATE")
  {
    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      if (aggregate.getGroupSet().isEmpty() || aggregate.getGroupSets().size() != 1) {
        return;
      }
      final RelTraitSet traitSet = aggregate.getTraitSet();
      if (traitSet.getTrait(RelCollationTraitDef.INSTANCE) == RelCollations.EMPTY) {
        List<RelFieldCollation> collations = Lists.newArrayList();
        aggregate.getGroupSet().forEachInt(x -> collations.add(new RelFieldCollation(x)));
        call.transformTo(aggregate.copy(traitSet.plus(RelCollations.of(collations)), aggregate.getInputs()));
        call.getPlanner().prune(aggregate);
      }
    }
  };

  // RelOptRules.BASE_RULES
  static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          CoreRules.AGGREGATE_STAR_TABLE,
          CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
//          CalciteSystemProperty.COMMUTE.value()
//          ? CoreRules.JOIN_ASSOCIATE
//          : CoreRules.PROJECT_MERGE,
//          CoreRules.FILTER_SCAN,
//          CoreRules.PROJECT_FILTER_TRANSPOSE,
//          CoreRules.FILTER_PROJECT_TRANSPOSE,
//          CoreRules.FILTER_INTO_JOIN,
//          CoreRules.JOIN_PUSH_EXPRESSIONS,
//          CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
//          CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT,
//          CoreRules.AGGREGATE_CASE_TO_FILTER,
//          CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
//          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
//          CoreRules.PROJECT_WINDOW_TRANSPOSE,
          CoreRules.MATCH,
//          CoreRules.JOIN_COMMUTE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          CoreRules.SORT_PROJECT_TRANSPOSE,
          CoreRules.SORT_JOIN_TRANSPOSE,
          CoreRules.SORT_REMOVE_CONSTANT_KEYS,
          CoreRules.SORT_UNION_TRANSPOSE,
          CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
          CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS
      );

  // RelOptRules.CONSTANT_REDUCTION_RULES
  static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
//          CoreRules.PROJECT_REDUCE_EXPRESSIONS,
//          CoreRules.FILTER_REDUCE_EXPRESSIONS,
//          CoreRules.CALC_REDUCE_EXPRESSIONS,
//          CoreRules.WINDOW_REDUCE_EXPRESSIONS,
//          CoreRules.JOIN_REDUCE_EXPRESSIONS,
          CoreRules.FILTER_VALUES_MERGE,
          CoreRules.PROJECT_FILTER_VALUES_MERGE,
          CoreRules.PROJECT_VALUES_MERGE,
          CoreRules.AGGREGATE_VALUES);

  // RelOptRules.ABSTRACT_RELATIONAL_RULES
  static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES =
      ImmutableList.of(
//          CoreRules.FILTER_INTO_JOIN,
//          CoreRules.JOIN_CONDITION_PUSH,
          AbstractConverter.ExpandConversionRule.INSTANCE,
//          CoreRules.JOIN_COMMUTE,
//          CoreRules.PROJECT_TO_SEMI_JOIN,
//          CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN,
//          CoreRules.JOIN_TO_SEMI_JOIN,
          CoreRules.AGGREGATE_REMOVE,
          CoreRules.UNION_TO_DISTINCT,
//          CoreRules.PROJECT_REMOVE,
//          CoreRules.PROJECT_AGGREGATE_MERGE,
//          CoreRules.AGGREGATE_JOIN_TRANSPOSE,
          CoreRules.AGGREGATE_MERGE,
          CoreRules.AGGREGATE_PROJECT_MERGE,
          CoreRules.CALC_REMOVE,
          CoreRules.SORT_REMOVE);

  // RelOptRules.ABSTRACT_RULES
  static final List<RelOptRule> ABSTRACT_RULES =
      ImmutableList.of(
          CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
          CoreRules.UNION_PULL_UP_CONSTANTS,
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
          PruneEmptyRules.EMPTY_TABLE_INSTANCE,
          CoreRules.UNION_MERGE,
          CoreRules.INTERSECT_MERGE,
          CoreRules.MINUS_MERGE,
//          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
//          CoreRules.FILTER_MERGE,
          DateRangeRules.FILTER_INSTANCE,
          CoreRules.INTERSECT_TO_DISTINCT
      );

  private Rules()
  {
    // No instantiation.
  }

  // https://github.com/apache/druid/pull/10120
  private static final int HEP_DEFAULT_MATCH_LIMIT = 1200;

  private static final boolean DAG = false;
  private static final boolean NO_DAG = true;

  public static List<Program> programs(QueryMaker queryMaker)
  {
    PlannerContext context = queryMaker.getPlannerContext();
    return ImmutableList.of(druidPrograms(context, queryMaker));
  }

  private static Program druidPrograms(PlannerContext plannerContext, QueryMaker queryMaker)
  {
    PlannerConfig config = plannerContext.getPlannerConfig();

    List<Program> programs = Lists.newArrayList();
    programs.add(Programs.subQuery(DruidMetadataProvider.INSTANCE));  // uses minRow/maxRow/uniqueKey
    programs.add(DECORRELATE_TRIM_PROGRAM);

    programs.add(hepProgram(PreFilteringRule.instance(), CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW));

    programs.add(hepProgram(
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.FILTER_INTO_JOIN_DUMB,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.JOIN_PUSH_EXPRESSIONS,
        CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES
    ));
    programs.add(hepProgram(CoreRules.FILTER_MERGE));

    if (config.isUseJoinReordering()) {
      // from Programs.heuristicJoinOrder
      Program program = Programs.sequence(
          hepProgram(DruidMetadataProvider.INSTANCE, HepMatchOrder.BOTTOM_UP, CoreRules.JOIN_TO_MULTI_JOIN),
          hepProgram(DruidMetadataProvider.INSTANCE, CoreRules.MULTI_JOIN_OPTIMIZE)
      );
      programs.add((planner, rel, traits, materializations, lattices) -> {
        DruidMetadataProvider.CONTEXT.set(queryMaker);
        try {
          return RelOptUtil.countJoins(rel) > 1 ? program.run(planner, rel, traits, materializations, lattices) : rel;
        }
        finally {
          DruidMetadataProvider.CONTEXT.remove();
        }}
      );
    }
    programs.add(hepProgram(
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_REMOVE,
        CoreRules.PROJECT_WINDOW_TRANSPOSE,
        ProjectJoinTransposeRule.Config.DEFAULT.withPreserveExprCondition(Rules::pushdown).toRule()
    ));

    programs.add(hepProgram(
        ReduceExpressionsRule.PROJECT_REDUCE,
        CoreRules.FILTER_REDUCE_EXPRESSIONS,
        CoreRules.CALC_REDUCE_EXPRESSIONS,
        CoreRules.WINDOW_REDUCE_EXPRESSIONS,
        CoreRules.JOIN_REDUCE_EXPRESSIONS
    ));

    if (!config.isUseApproximateCountDistinct()) {
      // We'll need this to expand COUNT DISTINCTs.
      // Avoid AggregateExpandDistinctAggregatesRule.INSTANCE; it uses grouping function, and we don't support those.
      programs.add(hepProgram(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN));
    }

    programs.add(hepProgram(
        CoreRules.AGGREGATE_CASE_TO_FILTER, CoreRules.PROJECT_AGGREGATE_MERGE, CoreRules.AGGREGATE_JOIN_TRANSPOSE
    ));

    // collation is removed by rel builder (AggregateExpandDistinctAggregatesRule, AggregateCaseToFilterRule, etc.)
    programs.add(hepProgram(SET_COLLATION_ON_AGGREGATE));

    programs.add(Programs.ofRules(druidConventionRuleSet(plannerContext, queryMaker)));

    programs.add(hepProgram(DruidJoinProjectRule.INSTANCE));

    Program program = Programs.sequence(programs.toArray(new Program[0]));
    return config.isDumpPlan() ? Dump.wrap(program) : program;
  }

  private static final EnumSet<SqlKind> ALLOWED =  EnumSet.of(SqlKind.INPUT_REF, SqlKind.OTHER_FUNCTION, SqlKind.LIKE);

  private static boolean pushdown(RexNode rex)
  {
    return rex.getKind() == SqlKind.LITERAL || ALLOWED.contains(rex.getKind()) && Utils.isSoleRef(rex);
  }

  private static Program hepProgram(RelOptRule... rules)
  {
    return hepProgram(HepMatchOrder.TOP_DOWN, rules);
  }

  private static Program hepProgram(RelMetadataProvider provider, RelOptRule... rules)
  {
    return hepProgram(provider, HepMatchOrder.TOP_DOWN, rules);
  }

  private static Program hepProgram(HepMatchOrder order, RelOptRule... rules)
  {
    return hepProgram(DefaultRelMetadataProvider.INSTANCE, order, rules);
  }

  private static Program hepProgram(RelMetadataProvider provider, HepMatchOrder order, RelOptRule... rules)
  {
    HepProgram program = new HepProgramBuilder()
        .addMatchOrder(order)
        .addRuleCollection(Arrays.asList(rules))
        .addMatchLimit(HEP_DEFAULT_MATCH_LIMIT)
        .build();
    return Programs.of(program, DAG, provider);
  }

  private static Program relLogProgram(final String subject)
  {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      LOG.info("<<<< %s >>>>\n%s", subject, DSuppliers.lazyLog(() -> RelOptUtil.toString(rel)));
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
    static Program wrap(Program program) {return new Dump(program);}

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
    rules.add(new DruidFilterableTableScanRule(queryMaker));
    rules.add(new DruidProjectableTableScanRule(queryMaker));
    rules.add(new DruidTableScanRule(queryMaker));
    rules.add(new DruidTableFunctionScanRule(queryMaker));
    rules.add(new DruidValuesRule(queryMaker));

    rules.addAll(DruidRules.RULES);
    rules.add(DruidRelToDruidRule.instance());

    return ImmutableList.copyOf(rules);
  }

  private static List<RelOptRule> baseRuleSet(PlannerContext plannerContext)
  {
    final PlannerConfig plannerConfig = plannerContext.getPlannerConfig();
    final List<RelOptRule> rules = Lists.newArrayList();

    // Calcite rules.
    rules.addAll(DEFAULT_RULES);
    rules.addAll(CONSTANT_REDUCTION_RULES);
    rules.addAll(ABSTRACT_RELATIONAL_RULES);
    rules.addAll(ABSTRACT_RULES);

    if (plannerConfig.isUseJoinReordering()) {
      rules.remove(JoinPushThroughJoinRule.RIGHT);
      rules.remove(JoinPushThroughJoinRule.LEFT);
    }

    rules.add(SortCollapseRule.instance());
    rules.add(ProjectAggregatePruneUnusedCallRule.instance());

    return rules;
  }
}
