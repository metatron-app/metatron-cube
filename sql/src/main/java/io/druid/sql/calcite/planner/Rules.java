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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.logger.Logger;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.rule.AggregateMergeRule;
import io.druid.sql.calcite.rule.DruidCost;
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
import io.druid.sql.calcite.rule.SortCollapseRule;
import io.druid.sql.calcite.table.DruidTable;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MatchRule;
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
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

public class Rules
{
  static final Logger LOG = new Logger(DruidPlanner.class);

  public static final int DRUID_CONVENTION_RULES = 0;

  public static final RelOptRule REDUCE_EXPR_PROJECT_INSTANCE =
      new ReduceExpressionsRule.ProjectReduceExpressionsRule(LogicalProject.class, true, RelFactories.LOGICAL_BUILDER)
      {
        public void onMatch(RelOptRuleCall call)
        {
          final Project project = call.rel(0);
          if (Iterables.any(project.getProjects(), expr -> !expr.isA(SqlKind.INPUT_REF))) {
            super.onMatch(call);
          }
        }
      };

  // RelOptRules.BASE_RULES
  private static final List<RelOptRule> DEFAULT_RULES =
      ImmutableList.of(
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
//          TableScanRule.INSTANCE,
//          CalciteSystemProperty.COMMUTE.value() ? JoinAssociateRule.INSTANCE :
          ProjectMergeRule.INSTANCE,
//          FilterTableScanRule.INSTANCE,
//          ProjectFilterTransposeRule.INSTANCE,
//          FilterProjectTransposeRule.INSTANCE,
//          FilterJoinRule.FILTER_ON_JOIN,
//          JoinPushExpressionsRule.INSTANCE,
//          AggregateExpandDistinctAggregatesRule.INSTANCE,
//          AggregateCaseToFilterRule.INSTANCE,
//          AggregateReduceFunctionsRule.INSTANCE,
//          FilterAggregateTransposeRule.INSTANCE,
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

  // Rules from CalcitePrepareImpl's createPlanner. (Not Used)
  private static final List<RelOptRule> MISCELLANEOUS_RULES =
      ImmutableList.of(
          Bindables.BINDABLE_TABLE_SCAN_RULE,
          ProjectTableScanRule.INSTANCE,
          ProjectTableScanRule.INTERPRETER
      );

  // RelOptRules.CONSTANT_REDUCTION_RULES
  private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
      ImmutableList.of(
          REDUCE_EXPR_PROJECT_INSTANCE,
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
//          FilterJoinRule.FILTER_ON_JOIN,
//          FilterJoinRule.JOIN,
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

    programs.add(hepProgram(PreFilteringRule.instance()));

    programs.add(hepProgram(
        FilterProjectTransposeRule.INSTANCE,
        FilterAggregateTransposeRule.INSTANCE,
        FilterJoinRule.FILTER_ON_JOIN,
        FilterJoinRule.JOIN,
        JoinPushExpressionsRule.INSTANCE,
        JoinPushTransitivePredicatesRule.INSTANCE
    ));
    if (config.isUseJoinReordering()) {
      // from Programs.heuristicJoinOrder
      Program program = Programs.sequence(
          hepProgram(HepMatchOrder.BOTTOM_UP, JoinToMultiJoinRule.INSTANCE), hepProgram(LoptOptimizeJoinRule.INSTANCE)
      );
      programs.add(withDruidMeta((planner, rel, traits, materializations, lattices) ->
        RelOptUtil.countJoins(rel) > 1 ? program.run(planner, rel, traits, materializations, lattices) : rel
      ));
    }
    programs.add(hepProgram(
        ProjectMergeRule.INSTANCE, FilterMergeRule.INSTANCE,
        new ProjectJoinTransposeRule(expr -> Utils.isInputRef(expr), RelFactories.LOGICAL_BUILDER)
    ));

    programs.add(Programs.ofRules(druidConventionRuleSet(plannerContext, queryMaker)));

    programs.add(hepProgram(DruidJoinProjectRule.INSTANCE));

    Program program = Programs.sequence(programs.toArray(new Program[0]));
    return config.isDumpPlan() ? Dump.wrap(program) : program;
  }

  private static Program hepProgram(RelOptRule... rules)
  {
    return hepProgram(HepMatchOrder.TOP_DOWN, rules);
  }

  private static Program hepProgram(HepMatchOrder order, RelOptRule... rules)
  {
    return Programs.of(
        new HepProgramBuilder()
            .addMatchOrder(order)
            .addRuleCollection(Arrays.asList(rules))
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

  private static Program withDruidMeta(Program program)
  {
    return (planner, rel, traits, materializations, lattices) -> {
      JaninoRelMetadataProvider prev = RelMetadataQuery.THREAD_PROVIDERS.get();
      try {
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DRUID_META_PROVIDER));
        return program.run(planner, rel, traits, materializations, lattices);
      }
      finally {
        RelMetadataQuery.THREAD_PROVIDERS.set(prev);
      }
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

    if (!plannerConfig.isUseApproximateCountDistinct()) {
      // We'll need this to expand COUNT DISTINCTs.
      // Avoid AggregateExpandDistinctAggregatesRule.INSTANCE; it uses grouping sets and we don't support those.
      rules.add(AggregateExpandDistinctAggregatesRule.JOIN);
    }
    if (plannerConfig.isUseJoinReordering()) {
      rules.remove(JoinPushThroughJoinRule.RIGHT);
      rules.remove(JoinPushThroughJoinRule.LEFT);
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

  // later..
  public static final RelMetadataProvider DRUID_META_PROVIDER = ChainedRelMetadataProvider.of(
      ImmutableList.of(
          DruidCostModel.SOURCE, DruidRelMdSelectivity.SOURCE, DruidRelMdRowCount.SOURCE, DruidRelMdDistinctRowCount.SOURCE,
          DefaultRelMetadataProvider.INSTANCE
      )
  );

  public static class DruidCostModel implements MetadataHandler<BuiltInMetadata.NonCumulativeCost>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.NON_CUMULATIVE_COST.method, new DruidCostModel());

    @Override
    public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef()
    {
      return BuiltInMetadata.NonCumulativeCost.DEF;
    }

    public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq)
    {
      return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
    }

    public RelOptCost getNonCumulativeCost(Filter rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel);
      double cost = rc * Utils.rexEvalCost(rel.getCondition());
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Project rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      double cost = rc * Utils.rexEvalCost(rel.getProjects());
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Sort rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      double cost = 0;
      if (!rel.getChildExps().isEmpty()) {
        cost += rc * PartialDruidQuery.SORT_MULTIPLIER;
      }
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }

    public RelOptCost getNonCumulativeCost(Join rel, RelMetadataQuery mq)
    {
      RelNode left = rel.getLeft();
      RelNode right = rel.getRight();
      double rc1 = Math.max(mq.getRowCount(left), 1);
      double rc2 = Math.max(mq.getRowCount(right), 1);
//      ImmutableBitSet leftKeys = rel.analyzeCondition().leftSet();
//      ImmutableBitSet rightKeys = rel.analyzeCondition().rightSet();
//      double drc1 = mq.getDistinctRowCount(left, leftKeys, null);
//      double drc2 = mq.getDistinctRowCount(right, rightKeys, null);
//      System.out.printf("%s%s : %f/%f + %s%s : %f/%f\n",
//                        Utils.alias(left), Utils.columnNames(left, leftKeys), drc1, rc1,
//                        Utils.alias(right), Utils.columnNames(right, rightKeys), drc2, rc2);
      return DruidCost.FACTORY.makeCost(0, Utils.joinCost(rc1, rc2), 0);
    }

    public RelOptCost getNonCumulativeCost(Aggregate rel, RelMetadataQuery mq)
    {
      int groupings = rel.getGroupSets().size();
      int cardinality = rel.getGroupSet().cardinality();

      double rc = mq.getRowCount(rel.getInput());
      double cost = rc * Utils.aggregationCost(cardinality, rel.getAggCallList()) * groupings;
      return DruidCost.FACTORY.makeCost(0, cost, 0);
    }
  }

  public static class DruidRelMdSelectivity implements MetadataHandler<BuiltInMetadata.Selectivity>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new DruidRelMdSelectivity());

    @Override
    public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
      return BuiltInMetadata.Selectivity.DEF;
    }

    public Double getSelectivity(Filter rel, RelMetadataQuery mq, RexNode predicate)
    {
      return Utils.selectivity(rel.getCondition());
    }
  }

  public static class DruidRelMdDistinctRowCount implements MetadataHandler<BuiltInMetadata.DistinctRowCount>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTINCT_ROW_COUNT.method, new DruidRelMdDistinctRowCount());

    @Override
    public MetadataDef<BuiltInMetadata.DistinctRowCount> getDef() {
      return BuiltInMetadata.DistinctRowCount.DEF;
    }

    public Double getDistinctRowCount(RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate)
    {
      RelOptTable optTable = rel.getTable();
      if (optTable != null) {
        DruidTable table = optTable.unwrap(DruidTable.class);
        if (table != null) {
          long[] cardinalities = table.cardinalityRange(groupKey);
          if (cardinalities != null) {
            return (double) cardinalities[1];
          }
        }
      }
      return null;
    }
  }

  public static class DruidRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount>
  {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new DruidRelMdRowCount());

    @Override
    public MetadataDef<BuiltInMetadata.RowCount> getDef() {
      return BuiltInMetadata.RowCount.DEF;
    }

    public Double getRowCount(Join join, RelMetadataQuery mq)
    {
      JoinRelType type = join.getJoinType();
      if (!type.projectsRight()) {
        RexNode semiJoinSelectivity = RelMdUtil.makeSemiJoinSelectivityRexNode(mq, join);
        return NumberUtil.multiply(
            mq.getSelectivity(join.getLeft(), semiJoinSelectivity), mq.getRowCount(join.getLeft()));
      }
      double rc1 = Math.max(mq.getRowCount(join.getLeft()), 1);
      double rc2 = Math.max(mq.getRowCount(join.getRight()), 1);
      if (join.analyzeCondition().leftKeys.isEmpty()) {
        return rc1 * rc2;
      }
      if (type == JoinRelType.INNER) {
        double s1 = mq.getSelectivity(join.getLeft(), null);
        double s2 = mq.getSelectivity(join.getRight(), null);
        double delta = 1 - Math.abs(s1 - s2);
        if (s1 > s2) {
          rc1 *= delta;
        } else {
          rc2 *= delta;
        }
        if (rc1 > rc2) {
          rc1 /= 1 + Math.log10(rc1 / rc2);
        } else {
          rc2 /= 1 + Math.log10(rc2 / rc1);
        }
      }
      switch (type) {
        case INNER:
          return Math.max(rc1, rc2);
        case LEFT:
          return rc1;
        case RIGHT:
          return rc2;
        case FULL:
          return Math.max(rc1, rc2);
      }
      return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());
    }

    public Double getRowCount(Aggregate rel, RelMetadataQuery mq)
    {
      int groupings = rel.getGroupSets().size();
      int cardinality = rel.getGroupSet().cardinality();

      double rc = mq.getRowCount(rel.getInput());
      return rc * Utils.aggregationRow(cardinality) * groupings;
    }

    public Double getRowCount(Filter rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      return rc * Utils.selectivity(rel.getCondition());
    }

    public Double getRowCount(Sort rel, RelMetadataQuery mq)
    {
      double rc = mq.getRowCount(rel.getInput());
      if (rel.fetch != null) {
        rc = Math.min(rc, RexLiteral.intValue(rel.fetch));
      }
      return rc;
    }
  }
}
