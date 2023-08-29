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

package io.druid.sql.calcite.aggregation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.ByteArray;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rel.CannotBuildQueryException;
import io.druid.sql.calcite.rel.Grouping;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import io.druid.sql.calcite.table.RowSignature;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Aggregations
{
  private final PlannerContext plannerContext;
  private final RowSignature signature;

  private final Project project;
  private final Aggregate aggregate;

  private final RexBuilder rexBuilder;
  private final boolean finalizeAggregations;

  private final List<DimensionExpression> dimensions = Lists.newArrayList();
  private final BitSet removedDimensions = new BitSet();
  private final GroupingSetSpec groupingSet;

  private Granularity granularity;
  private HavingSpec having;

  private final RowSignature output;

  private final VColumnRegistry virtualColumns = new VColumnRegistry();
  private final Map<ByteArray, AggregatorFactory> aggregators = Maps.newLinkedHashMap();
  private final List<PostAggregator> postAggregators = Lists.newArrayList();

  public Aggregations(
      PlannerContext plannerContext,
      RowSignature signature,
      RexBuilder rexBuilder,
      PartialDruidQuery query,
      boolean finalizeAggregations
  )
  {
    this.plannerContext = plannerContext;
    this.signature = signature;
    this.rexBuilder = rexBuilder;
    this.project = query.getScanProject();
    this.aggregate = query.getAggregate();
    this.finalizeAggregations = finalizeAggregations;
    this.groupingSet = computeGroupingSetSpec();
    this.output = computeDimensions().computeNotProjectedConstantDimensions(query)
                                     .registerVirtualColumnDimensions(query)
                                     .computeAggregations(query);
  }

  public PlannerContext context()
  {
    return plannerContext;
  }

  private GroupingSetSpec computeGroupingSetSpec()
  {
    switch (aggregate.getGroupType()) {
      case SIMPLE:
        return GroupingSetSpec.EMPTY;
      case ROLLUP:
        return GroupingSetSpec.ROLLUP;
      case CUBE:
        return GroupingSetSpec.CUBE;
    }
    ImmutableBitSet groupBySet = aggregate.getGroupSet();
    Int2IntMap mapping = new Int2IntOpenHashMap();
    for (int x = groupBySet.nextSetBit(0); x >= 0; x = groupBySet.nextSetBit(x + 1)) {
      mapping.put(x, mapping.size());
    }
    List<List<Integer>> groups = Lists.newArrayList();
    for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
      List<Integer> indices = Lists.newArrayList();
      for (int x = groupSet.nextSetBit(0); x >= 0; x = groupSet.nextSetBit(x + 1)) {
        int index = mapping.getOrDefault(x, -1);
        if (index < 0) {
          throw new IAE("invalid index %d in %s ", x, groupBySet);
        }
        indices.add(index);
      }
      groups.add(indices);
    }
    return new GroupingSetSpec.Indices(groups);
  }

  public Aggregations computeDimensions()
  {
    int ix = 0;
    final String prefix = Calcites.findUnusedPrefix("d", signature.getColumnNames());
    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final String outputName = prefix + ix++;
      final RexNode rexNode = Expressions.fromFieldAccess(signature, project, i);
      final DruidExpression expression = toExpression(rexNode);
      if (expression == null) {
        throw new CannotBuildQueryException(aggregate, rexNode);
      }
      dimensions.add(new DimensionExpression(outputName, expression, Calcites.asValueDesc(rexNode)));
      if (i == 0 && !expression.isSimpleExtraction()) {
        granularity = Expressions.asGranularity(expression, signature);
        if (granularity != null) {
          removedDimensions.set(i);
          postAggregators.add(0, GuavaUtils.lastOf(dimensions).toPostAggregator());   // for timestamp accessing post processors
        }
      }
    }
    return this;
  }

  private Aggregations computeNotProjectedConstantDimensions(PartialDruidQuery query)
  {
    Project project = GuavaUtils.nvl(query.getAggregateProject(), query.getSortProject());
    if (project != null) {
      ImmutableBitSet bitSet = RelOptUtil.InputFinder.bits(project.getProjects(), null);
      for (int i = 0; i < dimensions.size(); i++) {
        if (!bitSet.get(i) && !removedDimensions.get(i) && dimensions.get(i).isConstant()) {
          removedDimensions.set(i);
        }
      }
    }
    return this;
  }

  private Aggregations registerVirtualColumnDimensions(PartialDruidQuery query)
  {
    for (int i = 0; i < dimensions.size(); i++) {
      DimensionExpression dimension = dimensions.get(i);
      if (!removedDimensions.get(i) && !dimension.getDruidExpression().isSimpleExtraction()) {
        virtualColumns.register(dimension.getOutputName(), dimension.getDruidExpression());
      }
    }
    return this;
  }

  private RowSignature computeAggregations(PartialDruidQuery query)
  {
    RowSignature signature = translateAggregateCall(query.getAggregate());
    if (query.getAggregateFilter() != null) {
      this.having = computeHavingFilter(query.getAggregateFilter(), signature);
    }
    if (query.getAggregateProject() != null) {
      signature = computePostProject(query.getAggregateProject(), "p", signature);
    }
    return signature;
  }

  private RowSignature translateAggregateCall(Aggregate aggregate)
  {
    String prefix = Calcites.findUnusedPrefix("a", signature.getColumnNames());
    List<AggregateCall> calls = aggregate.getAggCallList();
    List<String> outputNames = Lists.newArrayList(Iterables.transform(dimensions, DimensionExpression::getOutputName));
    for (int i = 0; i < calls.size(); i++) {
      String outputName = prefix + i;
      if (!translateAggregateCall(calls.get(i), outputName)) {
        throw new CannotBuildQueryException(aggregate, calls.get(i));
      }
      outputNames.add(outputName);
    }
    return RowSignature.from(outputNames, aggregate.getRowType(), asTypeSolver());
  }

  private boolean translateAggregateCall(AggregateCall call, String outputName)
  {
    DimFilter filter = null;
    if (call.filterArg >= 0) {
      if (project == null) {
        return false;
      }
      filter = toAggregateFilter(call.filterArg);
      if (filter == null) {
        return false;
      }
    }
    return plannerContext.getOperatorTable().lookupAggregator(this, outputName, call, filter);
  }

  @Nullable
  private DimFilter toAggregateFilter(int filterOn)
  {
    final RexBuilder builder = project.getCluster().getRexBuilder();
    final RexNode expression = project.getProjects().get(filterOn);
    final DimFilter filter = Expressions.toFilter(plannerContext, signature, builder, expression);
    if (filter != null) {
      return Filtration.create(filter).optimizeFilterOnly(signature).getDimFilter();
    }
    return null;
  }

  private HavingSpec computeHavingFilter(Filter filter, RowSignature signature)
  {
    final DruidExpression expression = Expressions.toDruidExpression(plannerContext, signature, filter.getCondition());
    if (expression == null) {
      throw new CannotBuildQueryException(filter, filter.getCondition());
    }
    return new ExpressionHavingSpec(expression.getExpression());
  }

  public RowSignature computePostProject(Project project, String base, RowSignature signature)
  {
    if (project == null) {
      return signature;
    }
    int ix = 0;
    final List<String> outputNames = new ArrayList<>();
    final String prefix = Calcites.findUnusedPrefix(base, signature.getColumnNames());
    for (RexNode rexNode : project.getProjects()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final DruidExpression expression = toExpression(signature, rexNode);
      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      }
      if (expression.isDirectColumnAccess()) {
        outputNames.add(expression.getDirectColumn());
      } else if (expression.getExpression() != null) {
        String outputName = prefix + ix++;
        register(new MathPostAggregator(outputName, expression.getExpression()));
        outputNames.add(outputName);
      } else {
        throw new UnsupportedOperationException("?");
      }
    }
    return RowSignature.from(outputNames, project.getRowType(), asTypeSolver());
  }

  public boolean isFinalizing()
  {
    return finalizeAggregations;
  }

  public ValueDesc resolve(String column)
  {
    return signature.resolve(column);
  }

  @Nullable
  public List<DruidExpression> argumentsToExpressions(List<Integer> fields)
  {
    List<DruidExpression> expressions = fields.stream()
                                              .map(this::fieldToExpression).filter(v -> v != null)
                                              .collect(Collectors.toList());
    return expressions.size() == fields.size() ? expressions : null;
  }

  @Nullable
  public static List<DruidExpression> argumentsToExpressions(
      List<Integer> fields,
      PlannerContext plannerContext,
      RowSignature signature,
      Project project
  )
  {
    List<DruidExpression> expressions =
        fields.stream()
              .map(x -> Expressions.fromFieldAccess(signature, project, x))
              .map(rex -> Expressions.toDruidExpression(plannerContext, signature, rex))
              .filter(v -> v != null)
              .collect(Collectors.toList());
    return expressions.size() == fields.size() ? expressions : null;
  }

  @Nullable
  public DruidExpression fieldToExpression(int field)
  {
    return toExpression(Expressions.fromFieldAccess(signature, project, field));
  }

  @Nullable
  public DruidExpression toExpression(RexNode rexNode)
  {
    return Expressions.toDruidExpression(plannerContext, signature, rexNode);
  }

  @Nullable
  public DruidExpression toExpression(RowSignature signature, RexNode rexNode)
  {
    return Expressions.toDruidExpression(plannerContext, signature, rexNode);
  }

  public String registerColumn(String outputName, DruidExpression expression)
  {
    if (expression.isDirectColumnAccess()) {
      return expression.getDirectColumn();
    }
    return virtualColumns.register(outputName, expression).getOutputName();
  }

  public DimensionSpec registerDimension(String outputName, DruidExpression expression)
  {
    if (expression.isSimpleExtraction()) {
      return expression.getSimpleExtraction().toDimensionSpec(null);
    }
    return DefaultDimensionSpec.of(virtualColumns.register(outputName, expression).getOutputName());
  }

  public void register(AggregatorFactory factory, DimFilter predicate)
  {
    factory = AggregatorFactory.wrap(factory, predicate);
    AggregatorFactory prev = aggregators.putIfAbsent(ByteArray.wrap(factory.getCacheKey()), factory);
    if (prev != null) {
      postAggregators.add(FieldAccessPostAggregator.of(factory.getName(), prev));
    }
  }

  public void register(PostAggregator postAggregator)
  {
    postAggregators.add(postAggregator);
  }

  public boolean useApproximateCountDistinct()
  {
    return plannerContext.getPlannerConfig().isUseApproximateCountDistinct();
  }

  public RexNode toFieldRef(int field)
  {
    return Expressions.fromFieldAccess(signature, project, field);
  }

  public RexNode makeCall(SqlOperator op, RexNode... exprs)
  {
    return rexBuilder.makeCall(op, exprs);
  }

  @Nullable
  public DimFilter toFilter(RexNode rexNode)
  {
    DimFilter filter = Expressions.toFilter(plannerContext, signature, rexBuilder, rexNode);
    return filter == null ? null : Filtration.create(filter).optimizeFilterOnly(signature).getDimFilter();
  }

  public RowSignature asTypeSolver()
  {
    RowSignature.Builder builder = RowSignature.builderFrom(signature);
    for (VirtualColumn vc : virtualColumns.virtualColumns()) {
      builder.add(vc.getOutputName(), vc.resolveType(signature));
    }
    for (AggregatorFactory factory : aggregators.values()) {
      builder.add(factory.getName(), factory.getOutputType());
    }
    return builder.build();
  }

  public RowSignature getOutputRowSignature()
  {
    return output;
  }

  public Grouping build()
  {
    List<DimensionSpec> dimensionSpecs = IntStream.range(0, dimensions.size())
                                                  .filter(x -> !removedDimensions.get(x))
                                                  .mapToObj(x -> dimensions.get(x).toDimensionSpec(virtualColumns))
                                                  .collect(Collectors.toList());
    return new Grouping(
        granularity,
        dimensions,
        dimensionSpecs,
        groupingSet,
        virtualColumns.simplify(aggregators.values()),
        virtualColumns.virtualColumns(),
        postAggregators,
        having,
        output
    );
  }

  public DruidExpression getNonDistinctSingleArgument(AggregateCall call)
  {
    if (!call.isDistinct() && call.getArgList().size() == 1) {
      return fieldToExpression(call.getArgList().get(0));
    }
    return null;
  }
}
