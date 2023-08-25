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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// copied from org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePreFilteringRule
public class PreFilteringRule extends RelOptRule
{
  public static PreFilteringRule instance()
  {
    return new PreFilteringRule();
  }

  private final Set<RelNode> visited = Sets.newIdentityHashSet();

  private PreFilteringRule()
  {
    super(DruidRel.operand(Filter.class, DruidRel.operand(Join.class)));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter filter = call.rel(0);
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    // 0. Register that we have visited this operator in this rule
    if (!visited.add(filter) || !visited.add(call.rel(1))) {
      return;
    }

    RexNode condition = filter.getCondition();

    // 1. Recompose filter possibly by pulling out common elements from DNF expressions
    RexNode topFilterCondition = RexUtil.pullFactors(rexBuilder, condition);
    if (!topFilterCondition.isA(SqlKind.AND)) {
      return;
    }

    // hack to keep ordering of pulled filters
    if (condition.isA(SqlKind.OR)) {
      List<RexNode> ordering = Lists.newArrayList();
      for (RexNode rexNode : RexUtil.flattenOr(Arrays.asList(Utils.operands(condition).get(0)))) {
        ordering.addAll(RelOptUtil.conjunctions(rexNode));
      }
      List<RexNode> operands = Lists.newArrayList(Utils.operands(topFilterCondition));
      for (RexNode rex : operands) {
        if (!ordering.contains(rex)) {
          ordering.add(rex);
        }
      }
      Collections.sort(operands, Ordering.explicit(ordering));
      topFilterCondition= RexUtil.composeConjunction(rexBuilder, operands);
    }

    // 2. We extract possible candidates to be pushed down
    List<RexNode> operandsToPushDown = new ArrayList<>();
    List<RexNode> deterministicExprs = new ArrayList<>();
    List<RexNode> nonDeterministicExprs = new ArrayList<>();

    switch (topFilterCondition.getKind()) {
      case AND:
        ImmutableList<RexNode> operands = RexUtil.flattenAnd(Utils.operands(topFilterCondition));

        Set<String> operandsToPushDownDigest = new HashSet<String>();

        for (RexNode operand : operands) {
          if (operand.getKind() == SqlKind.OR) {
            for (RexNode extractedExpr : extractCommonOperands(rexBuilder, operand)) {
              if (operandsToPushDownDigest.add(extractedExpr.toString())) {
                operandsToPushDown.add(extractedExpr);
              }
            }
          }

          // TODO: Make expr traversal recursive. Extend to traverse inside
          // elements of DNF/CNF & extract more deterministic pieces out.
          if (isDeterministic(operand)) {
            deterministicExprs.add(operand);
          } else {
            nonDeterministicExprs.add(operand);
          }
        }

        // Pull out Deterministic exprs from non-deterministic and push down
        // deterministic expressions as a separate filter
        // NOTE: Hive by convention doesn't pushdown non-deterministic expressions
        if (!nonDeterministicExprs.isEmpty()) {
          for (RexNode expr : deterministicExprs) {
            if (operandsToPushDownDigest.add(expr.toString())) {
              operandsToPushDown.add(expr);
            }
          }

          topFilterCondition = RexUtil.pullFactors(
              rexBuilder, RexUtil.composeConjunction(rexBuilder, nonDeterministicExprs)
          );
        }
        break;

      case OR:
        operandsToPushDown = extractCommonOperands(rexBuilder, topFilterCondition);
        break;
    }

    // 2. If we did not generate anything for the new predicate, we bail out
    if (operandsToPushDown.isEmpty()) {
      return;
    }

    // 3. If the new conjuncts are already present in the plan, we bail out
    RexNode newPredicate = RexUtil.composeConjunction(
        rexBuilder, getPredsNotPushedAlready(filter.getInput(), operandsToPushDown)
    );
    if (newPredicate.isAlwaysTrue()) {
      return;
    }

    // 4. Otherwise, we create a new condition
    RexNode newChildFilterCondition = RexUtil.pullFactors(rexBuilder, newPredicate);

    // 5. We create the new filter that might be pushed down
    RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
    RelNode newChildFilter = factory.createFilter(filter.getInput(), newChildFilterCondition, ImmutableSet.of());
    RelNode newTopFilter = factory.createFilter(newChildFilter, topFilterCondition, ImmutableSet.of());

    // 6. We register both, so we do not fire the rule on them again
    visited.add(newChildFilter);
    visited.add(newTopFilter);

    call.transformTo(newTopFilter);
    call.getPlanner().prune(filter);
  }

  private static List<RexNode> extractCommonOperands(RexBuilder rexBuilder, RexNode condition)
  {
    assert condition.getKind() == SqlKind.OR;
    Multimap<String, RexNode> reductionCondition = LinkedHashMultimap.create();

    // Data structure to control whether a certain reference is present in every operand
    Set<String> refsInAllOperands = null;

    // 1. We extract the information necessary to create the predicate for the
    // new filter; currently we support comparison functions, in and between
    for (RexNode operand : RexUtil.flattenOr(Utils.operands(condition))) {
      boolean relational = Utils.isRelational(operand);
      RexNode operandCNF = relational ? RexUtil.toCnf(rexBuilder, -1, operand) : operand;
      List<RexNode> conjunctions = relational ? RelOptUtil.conjunctions(operandCNF) : Arrays.asList(operand);

      // We do not know what it is, we bail out for safety
      if (!conjunctions.stream().allMatch(PreFilteringRule::isDeterministic)) {
        return Collections.emptyList();
      }

      Set<String> refsInCurrentOperand = Sets.newLinkedHashSet();
      for (RexNode conjunction : conjunctions) {
        ImmutableBitSet refs = RelOptUtil.InputFinder.bits(conjunction);
        if (refs.cardinality() != 1) {
          // We do not know what it is, we bail out for safety
          return Collections.emptyList();
        }
        String stringRef = RexInputRef.createName(refs.nextSetBit(0));
        reductionCondition.put(stringRef, conjunction);
        refsInCurrentOperand.add(stringRef);
      }

      // Updates the references that are present in every operand up till now
      refsInAllOperands = refsInAllOperands == null ? refsInCurrentOperand :
                          Sets.intersection(refsInAllOperands, refsInCurrentOperand);

      // If we did not add any factor or there are no common factors, we can bail out
      if (refsInAllOperands.isEmpty()) {
        return Collections.emptyList();
      }
    }

    // 2. We gather the common factors and return them
    List<RexNode> commonOperands = new ArrayList<>();
    for (String ref : refsInAllOperands) {
      commonOperands.add(RexUtil.composeDisjunction(rexBuilder, reductionCondition.get(ref), false));
    }
    return commonOperands;
  }

  private static boolean isDeterministic(RexNode expr)
  {
    RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true)
    {
      @Override
      public Void visitCall(RexCall call)
      {
        if (!call.getOperator().isDeterministic()) {
          throw new Util.FoundOne(call);
        }
        return super.visitCall(call);
      }
    };

    try {
      expr.accept(visitor);
    }
    catch (Util.FoundOne e) {
      return false;
    }
    return true;
  }

  private static ImmutableList<RexNode> getPredsNotPushedAlready(RelNode inp, List<RexNode> predsToPushDown)
  {
    final RelOptPredicateList predicates = inp.getCluster().getMetadataQuery().getPulledUpPredicates(inp);
    if (GuavaUtils.isNullOrEmpty(predicates.pulledUpPredicates)) {
      return ImmutableList.copyOf(predsToPushDown);
    }
    final ImmutableSet<String> alreadyPushedPreds = ImmutableSet.copyOf(Iterables.transform(
        predicates.pulledUpPredicates, Functions.toStringFunction()));
    final ImmutableList.Builder<RexNode> newConjuncts = ImmutableList.builder();
    for (RexNode r : predsToPushDown) {
      if (!alreadyPushedPreds.contains(r.toString())) {
        newConjuncts.add(r);
      }
    }
    return newConjuncts.build();
  }
}

