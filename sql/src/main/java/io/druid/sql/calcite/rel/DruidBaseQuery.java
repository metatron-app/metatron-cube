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

package io.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.IntList;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.WindowingSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.select.StreamQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work to develop this query is done
 * during construction, which may throw {@link CannotBuildQueryException}.
 */
public class DruidBaseQuery implements DruidQuery
{
  private static final Logger LOG = new Logger(DruidBaseQuery.class);

  private final DataSource dataSource;
  private final RowSignature sourceRowSignature;
  private final PlannerContext plannerContext;

  @Nullable
  private final TableFunction tableFunction;

  @Nullable
  private final Filtration filtration;

  @Nullable
  private final SelectProjection selectProjection;

  @Nullable
  private final Aggregations aggregations;

  @Nullable
  private final Limiting limiting;

  @Nullable
  private final SortProject sortProject;

  @Nullable
  private final RowSignature outputRowSignature;

  @Nullable
  private final RelDataType outputRowType;

  private final Query query;

  public DruidBaseQuery(
      final PartialDruidQuery partialQuery,
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    this.dataSource = dataSource;
    this.sourceRowSignature = sourceRowSignature;
    this.outputRowType = partialQuery.leafRel().getRowType();
    this.plannerContext = plannerContext;

    RowSignature inputRowSignature = sourceRowSignature;

    // Now the fun begins.
    this.filtration = computeWhereFilter(partialQuery, plannerContext, inputRowSignature);
    this.tableFunction = computeTableExplode(partialQuery, plannerContext, inputRowSignature);
    if (tableFunction != null) {
      inputRowSignature = tableFunction.getOutputRowSignature();
    }
    this.selectProjection = computeSelectProjection(partialQuery, plannerContext, inputRowSignature);
    if (selectProjection != null) {
      inputRowSignature = selectProjection.getOutputRowSignature();
    }

    this.aggregations = computeAggregations(partialQuery, plannerContext, inputRowSignature, rexBuilder, finalizeAggregations);
    if (aggregations != null) {
      inputRowSignature = aggregations.getOutputRowSignature();
    }

    this.limiting = computeSortLimit(partialQuery, plannerContext, inputRowSignature, rexBuilder);
    if (limiting != null) {
      inputRowSignature = limiting.getOutputRowSignature();
    }

    if (aggregations != null) {
      this.sortProject = null;
      inputRowSignature = aggregations.computePostProject(partialQuery.getSortProject(), "s", inputRowSignature);
    } else {
      this.sortProject = computeSortProject(partialQuery, plannerContext, inputRowSignature);
      if (sortProject != null) {
        inputRowSignature = sortProject.getOutputRowSignature();
      }
    }

    this.outputRowSignature = inputRowSignature;
    this.query = computeQuery();
  }

  @Nullable
  private static TableFunction computeTableExplode(
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext,
      RowSignature sourceRowSignature
  )
  {
    PartialDruidQuery tableFunction = partialQuery.getTableFunction();
    if (tableFunction == null) {
      return null;
    }
    final TableFunctionScan tableScan = (TableFunctionScan) tableFunction.getScan();
    final RexCall tableFn = (RexCall) tableScan.getCall();

    final IntList inputRefs = Utils.extractInputRef(tableFn.operands);
    if (inputRefs == null) {
      throw new CannotBuildQueryException(tableScan, tableFn);
    }
    RowSignature appending = sourceRowSignature.subset(inputRefs);
    Filtration filtration = toFiltration(plannerContext, appending, tableFunction.getScanFilter());
    return new TableFunction(
        tableFn.op.getName(), appending.getColumnNames(), filtration, sourceRowSignature.append(appending)
    );
  }

  @Nullable
  private static Filtration computeWhereFilter(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    return toFiltration(plannerContext, sourceRowSignature, partialQuery.getScanFilter());
  }

  private static Filtration toFiltration(PlannerContext plannerContext, RowSignature sourceRowSignature, Filter filter)
  {
    if (filter == null) {
      return Filtration.create(null);
    }
    final RexBuilder builder = filter.getCluster().getRexBuilder();
    final RexNode condition = filter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(plannerContext, sourceRowSignature, builder, condition);
    if (dimFilter == null) {
      throw new CannotBuildQueryException(filter, condition);
    }
    return Filtration.create(dimFilter).optimize(sourceRowSignature);
  }

  @Nullable
  private static SelectProjection computeSelectProjection(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    final Project project = partialQuery.getScanProject();
    if (project == null || partialQuery.getAggregate() != null) {
      return null;
    }

    final String prefix = Calcites.findUnusedPrefix("v", sourceRowSignature.getColumnNames());

    int virtualColumnNameCounter = 0;
    final List<String> rowOrder = new ArrayList<>();
    final List<VirtualColumn> virtualColumns = new ArrayList<>();

    for (RexNode rexNode : project.getProjects()) {
      final DruidExpression expression = Expressions.toDruidExpression(plannerContext, sourceRowSignature, rexNode);
      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      }
      if (expression.isDirectColumnAccess()) {
        rowOrder.add(expression.getDirectColumn());
      } else {
        String virtualColumnName = prefix + virtualColumnNameCounter++;
        virtualColumns.add(expression.toVirtualColumn(virtualColumnName));
        rowOrder.add(virtualColumnName);
      }
    }

    return new SelectProjection(virtualColumns, RowSignature.from(rowOrder, project.getRowType()));
  }

  @Nullable
  private static Aggregations computeAggregations(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    Aggregate aggregate = partialQuery.getAggregate();
    if (aggregate == null) {
      return null;
    }
    return new Aggregations(plannerContext, sourceRowSignature, rexBuilder, partialQuery, finalizeAggregations);
  }

  @Nullable
  private static List<Windowing> computeWindowing(
      final Window window,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder
  )
  {
    final List<String> rowOrdering = sourceRowSignature.getColumnNames();
    final List<Windowing> windowings = Lists.newArrayList();

    int counter = 0;
    for (Window.Group group : window.groups) {
      final List<String> partitionColumns = Lists.newArrayList();
      final List<OrderByColumnSpec> sortColumns = Lists.newArrayList();
      final List<String> expressions = Lists.newArrayList();

      for (int key : BitSets.toIter(group.keys)) {
        partitionColumns.add(rowOrdering.get(key));
      }

      for (RelFieldCollation orderKey : group.orderKeys.getFieldCollations()) {
        switch (orderKey.getDirection()) {
          case ASCENDING:
            sortColumns.add(OrderByColumnSpec.asc(rowOrdering.get(orderKey.getFieldIndex())));
            break;
          case DESCENDING:
            sortColumns.add(OrderByColumnSpec.desc(rowOrdering.get(orderKey.getFieldIndex())));
            break;
          default:
            throw new UOE("not supported direction [%s]", orderKey.getDirection());
        }
      }
      Integer increment = null;
      Integer offset = null;

      int startIx;
      if (group.lowerBound.isCurrentRow()) {
        startIx = 0;
      } else if (group.lowerBound.isUnbounded()) {
        startIx = Integer.MAX_VALUE;
      } else {
        Comparable value = RexLiteral.value(window.getConstants().get(
            Utils.soleInputRef(group.lowerBound.getOffset()) - rowOrdering.size()
        ));
        if (value instanceof Number) {
          startIx = ((Number) value).intValue();
        } else if (value instanceof NlsString){
          String string = ((NlsString) value).getValue();
          int ix1 = string.indexOf(':');
          int ix2 = string.indexOf(':', ix1 + 1);
          startIx = Integer.valueOf(string.substring(0, ix1));
          if (startIx < 0) {
            startIx = Integer.MAX_VALUE;
          }
          increment = Integer.valueOf(string.substring(ix1 + 1, ix2));
          offset = ix2 + 1 == string.length() ? null : Integer.valueOf(string.substring(ix2 + 1));
        } else {
          throw new UOE("cannot translate %s to window frame", value);
        }
      }
      int endIx;
      if (group.upperBound.isCurrentRow()) {
        endIx = 0;
      } else if (group.upperBound.isUnbounded()) {
        endIx = Integer.MAX_VALUE;
      } else {
        Comparable value = RexLiteral.value(window.getConstants().get(
            Utils.soleInputRef(group.upperBound.getOffset()) - rowOrdering.size()
        ));
        if (value instanceof Number) {
          endIx = ((Number) value).intValue();
        } else {
          throw new UOE("cannot translate %s to window frame", value);
        }
      }
      for (AggregateCall aggCall : group.getAggregateCalls(window)) {
        // todo filter
        final List<DruidExpression> arguments = Aggregations.argumentsToExpressions(
            aggCall.getArgList(), plannerContext, sourceRowSignature, null
        );
        arguments.add(DruidExpression.numberLiteral(-startIx));
        arguments.add(DruidExpression.numberLiteral(endIx));
        final String functionName = aggCall.getAggregation().getName();
        final String expression = DruidExpression.functionCall(
            !functionName.startsWith("$") ? "$" + functionName : functionName, arguments    // hack
        );
        expressions.add(StringUtils.format("\"%s\" = %s", aggCall.getName(), expression));
      }
      RowSignature outputRowSignature = RowSignature.from(window.getRowType());
      windowings.add(new Windowing(partitionColumns, sortColumns, increment, offset, expressions, outputRowSignature));
    }
    return windowings;
  }

  @Nullable
  private SortProject computeSortProject(
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext,
      RowSignature inputRowSignature
  )
  {
    if (partialQuery.getAggregate() != null) {
      return null;
    }
    final Project sortProject = partialQuery.getSortProject();
    if (sortProject == null) {
      return null;
    } else {
      List<String> sourceRows = inputRowSignature.getColumnNames();
      List<String> targetRows = Lists.newArrayList();
      for (RexNode rexNode : sortProject.getProjects()) {
        int index = ((RexInputRef) rexNode).getIndex();
        targetRows.add(sourceRows.get(index));
      }
      return new SortProject(targetRows, RowSignature.from(targetRows, sortProject.getRowType()));
    }
  }

  @Nullable
  private static HavingSpec computeHavingFilter(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature,
      final PlannerContext plannerContext
  )
  {
    final Filter havingFilter = partialQuery.getAggregateFilter();

    if (havingFilter == null) {
      return null;
    }

    final RexNode condition = havingFilter.getCondition();
    final DruidExpression expression = Expressions.toDruidExpression(
        plannerContext,
        outputRowSignature,
        condition
    );
    if (expression == null) {
      throw new CannotBuildQueryException(havingFilter, condition);
    } else {
      return new ExpressionHavingSpec(expression.getExpression());
    }
  }

  @Nullable
  private static Limiting computeSortLimit(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature inputRowSignature,
      final RexBuilder rexBuilder
  )
  {
    final Window window = partialQuery.getWindow();
    final Sort sort = partialQuery.getSort();
    if (window == null && sort == null) {
      return null;
    }

    RowSignature outputRowSignature = inputRowSignature;

    List<WindowingSpec> windowingSpecs = null;
    if (window != null) {
      windowingSpecs = Lists.newArrayList();
      List<Windowing> windowings = computeWindowing(window, plannerContext, inputRowSignature, rexBuilder);
      for (Windowing windowing : windowings) {
        windowingSpecs.add(windowing.asSpec());
      }
      outputRowSignature = GuavaUtils.lastOf(windowings).getOutputRowSignature();
      // window only appends aggregate rows (see LogicalWindow.create)
    }

    List<String> inputColumns = inputRowSignature.getColumnNames();
    List<String> outputColumns = outputRowSignature.getColumnNames();
    Map<String, String> alias = Maps.newHashMap();
    for (int i = 0; i < inputColumns.size(); i++) {
      if (!inputColumns.get(i).equals(outputColumns.get(i))) {
        alias.put(inputColumns.get(i), outputColumns.get(i));
      }
    }
    if (sort == null) {
      return new Limiting(windowingSpecs, null, -1, alias, inputRowSignature, outputRowSignature);
    }

    final Integer limit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;

    if (sort.offset != null) {
      // LimitSpecs don't accept offsets.
      throw new CannotBuildQueryException(sort);
    }

    // Extract orderBy column specs.
    final List<OrderByColumnSpec> orderings = asOrderingSpec(sort, inputRowSignature);
    return new Limiting(windowingSpecs, orderings, limit, alias, inputRowSignature, outputRowSignature);
  }

  private static List<OrderByColumnSpec> asOrderingSpec(Sort sort, RowSignature rowSignature)
  {
    final List<RexNode> children = sort.getSortExps();
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(children.size());
    for (int sortKey = 0; sortKey < children.size(); sortKey++) {
      final RexNode sortExpression = children.get(sortKey);
      final RelFieldCollation collation = sort.getCollation().getFieldCollations().get(sortKey);
      if (!sortExpression.isA(SqlKind.INPUT_REF)) {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        throw new CannotBuildQueryException(sort, sortExpression);
      }
      final RexInputRef ref = (RexInputRef) sortExpression;
      final String fieldName = rowSignature.getColumnNames().get(ref.getIndex());

      final Direction direction;

      if (collation.getDirection() == RelFieldCollation.Direction.ASCENDING) {
        direction = Direction.ASCENDING;
      } else if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
        direction = Direction.DESCENDING;
      } else {
        throw new UOE("not supported direction [%s]", collation.getDirection());
      }

      // use natural ordering.. whatsoever
      orderBys.add(new OrderByColumnSpec(fieldName, direction));
    }
    return orderBys;
  }

  @Override
  public RelDataType getOutputRowType()
  {
    return outputRowType;
  }

  @Override
  public RowSignature getInputRowSignature()
  {
    return sourceRowSignature;
  }

  @Override
  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  @Override
  public Query getQuery()
  {
    return query;
  }

  /**
   * Return this query as some kind of Druid query. The returned query will either be {@link TopNQuery},
   * {@link TimeseriesQuery}, {@link GroupByQuery}, or {@link StreamQuery}.
   *
   * @return Druid query
   */
  private Query computeQuery()
  {
    if (aggregations != null) {
      Preconditions.checkArgument(tableFunction == null, "todo: cannot handle table function in aggregation queries");

      final Grouping grouping = aggregations.build();
      final TimeseriesQuery tsQuery = toTimeseriesQuery(grouping);
      if (tsQuery != null) {
        return tsQuery;
      }

      final TopNQuery topNQuery = toTopNQuery(grouping);
      if (topNQuery != null) {
        return topNQuery;
      }

      return toGroupByQuery(grouping);
    }

    final StreamQuery scanQuery = toScanQuery();
    if (scanQuery != null) {
      return scanQuery;
    }

    throw new CannotBuildQueryException("Cannot convert query parts into an actual query");
  }

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @return query
   */
  @Nullable
  public TimeseriesQuery toTimeseriesQuery(Grouping grouping)
  {
    if (!grouping.getDimensionSpecs().isEmpty()) {
      return null;
    }
    boolean descending = false;
    if (grouping.getDimensions().size() == 1 && limiting != null && !GuavaUtils.isNullOrEmpty(limiting.getColumns())) {
      DimensionExpression dimension = grouping.getDimensions().get(0);
      List<OrderByColumnSpec> columns = limiting.getColumns();
      // We're ok if the first order by is time (since every time value is distinct, the rest of the columns
      // wouldn't matter anyway).
      OrderByColumnSpec firstOrderBy = columns.get(0);
      if (dimension.getOutputName().equals(firstOrderBy.getDimension())) {
        descending = firstOrderBy.getDirection() == Direction.DESCENDING;
      }
    }

    return new TimeseriesQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        grouping.getGranularity(),
        grouping.getVirtualColumns(),
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        grouping.getHavingFilter(),
        limiting == null ? null : limiting.getLimitSpec(),
        ImmutableList.copyOf(outputRowSignature.getColumnNames()),
        null,
        plannerContext.copyQueryContext()
    );
  }

  /**
   * Return this query as a TopN query, or null if this query is not compatible with TopN.
   *
   * @return query or null
   */
  @Nullable
  public TopNQuery toTopNQuery(Grouping grouping)
  {
    if (!Granularities.isAll(grouping.getGranularity())) {
      return null;
    }
    // Must GROUP-BY on one column, ORDER BY zero or one column, limit less than maxTopNLimit, and no HAVING.
    final boolean topNOk = grouping != null
                           && grouping.getDimensions().size() == 1
                           && limiting != null
                           && limiting.getColumns().size() <= 1
                           && limiting.getLimit() > 0
                           && limiting.getLimit() <= plannerContext.getPlannerConfig().getMaxTopNLimit()
                           && grouping.getHavingFilter() == null;

    if (!topNOk) {
      return null;
    }

    final DimensionExpression dimensionExpr = Iterables.getOnlyElement(grouping.getDimensions());
    final DimensionSpec dimensionSpec = dimensionExpr.toDimensionSpec();
    if (!dimensionSpec.getDimension().equals(Row.TIME_COLUMN_NAME) &&
        !ValueDesc.isDimension(sourceRowSignature.resolve(dimensionSpec.getDimension()))) {
      return null;
    }
    final OrderByColumnSpec limitColumn;
    if (limiting.getColumns().isEmpty()) {
      limitColumn = OrderByColumnSpec.asc(
          dimensionSpec.getOutputName(),
          Calcites.getStringComparatorForValueType(dimensionExpr.getOutputType())
      );
    } else {
      limitColumn = Iterables.getOnlyElement(limiting.getColumns());
    }
    final TopNMetricSpec topNMetricSpec;

    if (limitColumn.getDimension().equals(dimensionSpec.getOutputName())) {
      // DimensionTopNMetricSpec is exact; always return it even if allowApproximate is false.
      final DimensionTopNMetricSpec baseMetricSpec = new DimensionTopNMetricSpec(
          null,
          limitColumn.getDimensionOrder()
      );
      topNMetricSpec = limitColumn.getDirection() == Direction.ASCENDING
                       ? baseMetricSpec
                       : new InvertedTopNMetricSpec(baseMetricSpec);
    } else if (plannerContext.getPlannerConfig().isUseApproximateTopN()) {
      // ORDER BY metric
      final NumericTopNMetricSpec baseMetricSpec = new NumericTopNMetricSpec(limitColumn.getDimension());
      topNMetricSpec = limitColumn.getDirection() == Direction.ASCENDING
                       ? new InvertedTopNMetricSpec(baseMetricSpec)
                       : baseMetricSpec;
    } else {
      return null;
    }

    return new TopNQuery(
        dataSource,
        grouping.getVirtualColumns(),
        dimensionSpec,
        topNMetricSpec,
        limiting == null ? null : limiting.getLimit(),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        ImmutableList.copyOf(outputRowSignature.getColumnNames()),
        plannerContext.copyQueryContext()
    );
  }

  /**
   * Return this query as a GroupBy query.
   *
   * @return query
   */
  public GroupByQuery toGroupByQuery(Grouping grouping)
  {
    return new GroupByQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        grouping.getGranularity(),
        grouping.getDimensionSpecs(),
        grouping.getGroupingSets(),
        grouping.getVirtualColumns(),
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        grouping.getHavingFilter(),
        limiting == null ? null : limiting.getLimitSpec(),
        ImmutableList.copyOf(outputRowSignature.getColumnNames()),
        null,
        plannerContext.copyQueryContext()
    );
  }

  /**
   * Return this query as a Scan query.
   *
   * @return query
   */
  public StreamQuery toScanQuery()
  {
    boolean descending = false;
    if (limiting != null && !GuavaUtils.isNullOrEmpty(limiting.getColumns())) {
      descending = OrderByColumnSpec.desc(Row.TIME_COLUMN_NAME).equals(limiting.getColumns().get(0));
    }

    RowSignature source = sourceRowSignature;
    if (selectProjection != null) {
      source = selectProjection.getOutputRowSignature();
    }
    final List<String> rowOrder = source.getColumnNames();
    if (rowOrder.isEmpty()) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("Attempting to convert to Scan query without any columns?");
    }
    return new StreamQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        tableFunction == null ? null : tableFunction.asSpec(),
        ImmutableList.copyOf(rowOrder),
        selectProjection == null ? null : selectProjection.getVirtualColumns(),
        null,
        null,
        limiting == null ? null : limiting.getLimitSpec(),
        sortProject == null ? null : sortProject.getColumns(),
        plannerContext.copyQueryContext()
    );
  }
}
