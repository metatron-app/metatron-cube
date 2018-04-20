/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Parser;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.ViewDataSource;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.ExpressionHavingSpec;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.Direction;
import io.druid.query.ordering.StringComparators;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamRawQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.InvertedTopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.ExtractionFns;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rule.GroupByRules;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work to develop this query is done
 * during construction, which may throw {@link CannotBuildQueryException}.
 */
public class DruidQuery
{
  private final DataSource dataSource;
  private final RowSignature sourceRowSignature;
  private final PlannerContext plannerContext;

  private final DimFilter filter;
  private final SelectProjection selectProjection;
  private final Grouping grouping;
  private final RowSignature outputRowSignature;
  private final RelDataType outputRowType;
  private final LimitSpec limitSpec;

  public DruidQuery(
      final PartialDruidQuery partialQuery,
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder
  )
  {
    this.dataSource = dataSource;
    this.sourceRowSignature = sourceRowSignature;
    this.outputRowType = partialQuery.leafRel().getRowType();
    this.plannerContext = plannerContext;

    // Now the fun begins.
    this.filter = computeWhereFilter(partialQuery, sourceRowSignature, plannerContext);
    this.selectProjection = computeSelectProjection(partialQuery, plannerContext, sourceRowSignature);
    this.grouping = computeGrouping(partialQuery, plannerContext, sourceRowSignature, rexBuilder);

    if (this.selectProjection != null) {
      this.outputRowSignature = this.selectProjection.getOutputRowSignature();
    } else if (this.grouping != null) {
      this.outputRowSignature = this.grouping.getOutputRowSignature();
    } else {
      this.outputRowSignature = sourceRowSignature;
    }

    this.limitSpec = computeLimitSpec(partialQuery, this.outputRowSignature);
  }

  @Nullable
  private static DimFilter computeWhereFilter(
      final PartialDruidQuery partialQuery,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext
  )
  {
    final Filter whereFilter = partialQuery.getWhereFilter();

    if (whereFilter == null) {
      return null;
    }

    final RexNode condition = whereFilter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        sourceRowSignature,
        condition
    );
    if (dimFilter == null) {
      throw new CannotBuildQueryException(whereFilter, condition);
    } else {
      return dimFilter;
    }
  }

  @Nullable
  private static SelectProjection computeSelectProjection(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    final Project project = partialQuery.getSelectProject();

    if (project == null || partialQuery.getAggregate() != null) {
      return null;
    }

    final List<DruidExpression> expressions = new ArrayList<>();

    for (final RexNode rexNode : project.getChildExps()) {
      final DruidExpression expression = Expressions.toDruidExpression(
          plannerContext,
          sourceRowSignature,
          rexNode
      );

      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      } else {
        expressions.add(expression);
      }
    }

    final List<String> directColumns = new ArrayList<>();
    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final List<String> rowOrder = new ArrayList<>();

    final String virtualColumnPrefix = Calcites.findOutputNamePrefix(
        "v",
        new TreeSet<>(sourceRowSignature.getRowOrder())
    );
    int virtualColumnNameCounter = 0;

    for (DruidExpression expression : expressions) {
      if (expression.isDirectColumnAccess()) {
        directColumns.add(expression.getDirectColumn());
        rowOrder.add(expression.getDirectColumn());
      } else {
        final String virtualColumnName = virtualColumnPrefix + virtualColumnNameCounter++;
        virtualColumns.add(
            expression.toVirtualColumn(
                virtualColumnName
            )
        );
        rowOrder.add(virtualColumnName);
      }
    }

    return new SelectProjection(directColumns, virtualColumns, RowSignature.from(rowOrder, project.getRowType()));
  }

  @Nullable
  private static Grouping computeGrouping(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder
  )
  {
    final Aggregate aggregate = partialQuery.getAggregate();
    final Project postProject = partialQuery.getPostProject();

    if (aggregate == null) {
      return null;
    }

    final List<DimensionExpression> dimensions = computeDimensions(partialQuery, plannerContext, sourceRowSignature);
    final List<Aggregation> aggregations = computeAggregations(
        partialQuery,
        plannerContext,
        sourceRowSignature,
        rexBuilder
    );

    RowSignature aggregateRowSignature = RowSignature.from(
        ImmutableList.copyOf(
            Iterators.concat(
                dimensions.stream().map(DimensionExpression::getOutputName).iterator(),
                aggregations.stream().map(Aggregation::getOutputName).iterator()
            )
        ),
        aggregate.getRowType()
    );

    if (postProject == null) {
      final HavingSpec havingFilter = computeHavingFilter(
          partialQuery,
          aggregateRowSignature,
          plannerContext
      );
      return Grouping.create(dimensions, aggregations, havingFilter, aggregateRowSignature);
    } else {
      final List<String> rowOrder = new ArrayList<>();

      int outputNameCounter = 0;
      for (final RexNode postAggregatorRexNode : postProject.getChildExps()) {
        // Attempt to convert to PostAggregator.
        final DruidExpression postAggregatorExpression = Expressions.toDruidExpression(
            plannerContext,
            aggregateRowSignature,
            postAggregatorRexNode
        );

        if (postAggregatorExpression == null) {
          throw new CannotBuildQueryException(postProject, postAggregatorRexNode);
        }

        ValueDesc toExprType = Calcites.getValueDescForSqlTypeName(postAggregatorRexNode.getType().getSqlTypeName());
        if (postAggregatorDirectColumnIsOk(aggregateRowSignature, postAggregatorExpression, toExprType)) {
          // Direct column access, without any type cast as far as Druid's runtime is concerned.
          // (There might be a SQL-level type cast that we don't care about)
          rowOrder.add(postAggregatorExpression.getDirectColumn());
        } else {
          final String postAggregatorName = "p" + outputNameCounter++;
          final PostAggregator postAggregator = new MathPostAggregator(
              postAggregatorName,
              postAggregatorExpression.getExpression()
          );
          aggregations.add(Aggregation.create(postAggregator));
          rowOrder.add(postAggregator.getName());

          aggregateRowSignature = aggregateRowSignature.addColumn(postAggregator.getName(), toExprType);
        }
      }

      // Remove literal dimensions that did not appear in the projection. This is useful for queries
      // like "SELECT COUNT(*) FROM tbl GROUP BY 'dummy'" which some tools can generate, and for which we don't
      // actually want to include a dimension 'dummy'.
      final ImmutableBitSet postProjectBits = RelOptUtil.InputFinder.bits(postProject.getChildExps(), null);
      for (int i = dimensions.size() - 1; i >= 0; i--) {
        final DimensionExpression dimension = dimensions.get(i);
        if (Evals.isConstant(Parser.parse(dimension.getDruidExpression().getExpression())) && !postProjectBits.get(i)) {
          dimensions.remove(i);
        }
      }
      final HavingSpec havingFilter = computeHavingFilter(
          partialQuery,
          aggregateRowSignature,
          plannerContext
      );
      return Grouping.create(
          dimensions,
          aggregations,
          havingFilter,
          RowSignature.from(rowOrder, postProject.getRowType())
      );
    }
  }

  /**
   * Returns dimensions corresponding to {@code aggregate.getGroupSet()}, in the same order.
   *
   * @param partialQuery       partial query
   * @param plannerContext     planner context
   * @param sourceRowSignature source row signature
   *
   * @return dimensions
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<DimensionExpression> computeDimensions(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<DimensionExpression> dimensions = new ArrayList<>();
    final String outputNamePrefix = Calcites.findOutputNamePrefix("d", new TreeSet<>(sourceRowSignature.getRowOrder()));
    int outputNameCounter = 0;

    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final String dimOutputName = outputNamePrefix + outputNameCounter++;
      final RexNode rexNode = Expressions.fromFieldAccess(sourceRowSignature, partialQuery.getSelectProject(), i);
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          sourceRowSignature,
          rexNode
      );
      if (druidExpression == null) {
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
      final ValueDesc outputType = Calcites.getValueDescForSqlTypeName(sqlTypeName);
      if (outputType == null || !ValueDesc.isPrimitive(outputType)) {
        // Can't group on unknown or COMPLEX types.
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      dimensions.add(new DimensionExpression(dimOutputName, druidExpression, outputType));
    }

    return dimensions;
  }

  /**
   * Returns aggregations corresponding to {@code aggregate.getAggCallList()}, in the same order.
   *
   * @param partialQuery       partial query
   * @param plannerContext     planner context
   * @param sourceRowSignature source row signature
   * @param rexBuilder         calcite RexBuilder
   *
   * @return aggregations
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<Aggregation> computeAggregations(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<Aggregation> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findOutputNamePrefix("a", new TreeSet<>(sourceRowSignature.getRowOrder()));

    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final String aggName = outputNamePrefix + i;
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = GroupByRules.translateAggregateCall(
          plannerContext,
          sourceRowSignature,
          rexBuilder,
          partialQuery.getSelectProject(),
          aggCall,
          aggregations,
          aggName
      );

      if (aggregation == null) {
        throw new CannotBuildQueryException(aggregate, aggCall);
      }

      aggregations.add(aggregation);
    }

    return aggregations;
  }

  @Nullable
  private static HavingSpec computeHavingFilter(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature,
      final PlannerContext plannerContext
  )
  {
    final Filter havingFilter = partialQuery.getHavingFilter();

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
  private static LimitSpec computeLimitSpec(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature
  )
  {
    final Sort sort;

    if (partialQuery.getAggregate() == null) {
      sort = partialQuery.getSelectSort();
    } else {
      sort = partialQuery.getSort();
    }

    if (sort == null) {
      return null;
    }

    final Integer limit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(sort.getChildExps().size());

    if (sort.offset != null) {
      // LimitSpecs don't accept offsets.
      throw new CannotBuildQueryException(sort);
    }

    // Extract orderBy column specs.
    for (int sortKey = 0; sortKey < sort.getChildExps().size(); sortKey++) {
      final RexNode sortExpression = sort.getChildExps().get(sortKey);
      final RelFieldCollation collation = sort.getCollation().getFieldCollations().get(sortKey);
      final Direction direction;
      final String ordering;

      if (collation.getDirection() == RelFieldCollation.Direction.ASCENDING) {
        direction = Direction.ASCENDING;
      } else if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
        direction = Direction.DESCENDING;
      } else {
        throw new ISE("WTF?! Don't know what to do with direction[%s]", collation.getDirection());
      }

      final SqlTypeName sortExpressionType = sortExpression.getType().getSqlTypeName();
      if (SqlTypeName.NUMERIC_TYPES.contains(sortExpressionType)
          || SqlTypeName.TIMESTAMP == sortExpressionType
          || SqlTypeName.DATE == sortExpressionType) {
        ordering = StringComparators.NUMERIC_NAME;
      } else {
        ordering = StringComparators.LEXICOGRAPHIC_NAME;
      }

      if (sortExpression.isA(SqlKind.INPUT_REF)) {
        final RexInputRef ref = (RexInputRef) sortExpression;
        final String fieldName = outputRowSignature.getRowOrder().get(ref.getIndex());
        orderBys.add(new OrderByColumnSpec(fieldName, direction, ordering));
      } else {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        throw new CannotBuildQueryException(sort, sortExpression);
      }
    }

    return new LimitSpec(orderBys, limit);
  }

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param aggregateRowSignature signature of the aggregation
   * @param expression            post-aggregation expression
   * @param toExprType            type of RexNode for the post-aggregation expression
   *
   * @return yes or no
   */
  private static boolean postAggregatorDirectColumnIsOk(
      RowSignature aggregateRowSignature,
      DruidExpression expression,
      ValueDesc toExprType
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // Check if a cast is necessary.
    final ValueDesc fromExprType = aggregateRowSignature.getColumnType(expression.getDirectColumn());

    return toExprType.equals(fromExprType);
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    final List<VirtualColumn> retVal = new ArrayList<>();

    if (grouping != null) {
      for (DimensionExpression dimensionExpression : grouping.getDimensions()) {
        retVal.addAll(dimensionExpression.getVirtualColumns());
      }

      for (Aggregation aggregation : grouping.getAggregations()) {
        retVal.addAll(aggregation.getVirtualColumns());
      }
    } else if (selectProjection != null) {
      retVal.addAll(selectProjection.getVirtualColumns());
    }

    return retVal;
  }

  public Grouping getGrouping()
  {
    return grouping;
  }

  public LimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  public RelDataType getOutputRowType()
  {
    return outputRowType;
  }

  public RowSignature getSourceRowSignature()
  {
    return sourceRowSignature;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public Query getQuery()
  {
    return computeQuery();
  }

  /**
   * Return this query as some kind of Druid query. The returned query will either be {@link TopNQuery},
   * {@link TimeseriesQuery}, {@link GroupByQuery}, {@link ScanQuery}, or {@link SelectQuery}.
   *
   * @return Druid query
   */
  private Query computeQuery()
  {
    final TimeseriesQuery tsQuery = toTimeseriesQuery();
    if (tsQuery != null) {
      return tsQuery;
    }

    final TopNQuery topNQuery = toTopNQuery();
    if (topNQuery != null) {
      return topNQuery;
    }

    final GroupByQuery groupByQuery = toGroupByQuery();
    if (groupByQuery != null) {
      return groupByQuery;
    }

    final StreamRawQuery scanQuery = toScanQuery();
    if (scanQuery != null) {
      return scanQuery;
    }

    final SelectQuery selectQuery = toSelectQuery();
    if (selectQuery != null) {
      return selectQuery;
    }

    throw new CannotBuildQueryException("Cannot convert query parts into an actual query");
  }

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @return query
   */
  @Nullable
  public TimeseriesQuery toTimeseriesQuery()
  {
    if (grouping == null || grouping.getHavingFilter() != null) {
      return null;
    }

    final Granularity queryGranularity;
    final boolean descending;

    if (grouping.getDimensions().isEmpty()) {
      queryGranularity = Granularities.ALL;
      descending = false;
    } else if (grouping.getDimensions().size() == 1) {
      final DimensionSpec dimensionSpec = Iterables.getOnlyElement(grouping.getDimensions()).toDimensionSpec();
      final Granularity gran = ExtractionFns.toQueryGranularity(dimensionSpec.getExtractionFn());

      if (gran == null || !dimensionSpec.getDimension().equals(Column.TIME_COLUMN_NAME)) {
        // Timeseries only applies if the single dimension is granular __time.
        return null;
      } else {
        queryGranularity = gran;
      }

      if (limitSpec != null) {
        // If there is a limit spec, timeseries cannot LIMIT; and must be ORDER BY time (or nothing).

        if (limitSpec.getLimit() < Integer.MAX_VALUE) {
          return null;
        }

        if (limitSpec.getColumns().isEmpty()) {
          descending = false;
        } else {
          // We're ok if the first order by is time (since every time value is distinct, the rest of the columns
          // wouldn't matter anyway).
          final OrderByColumnSpec firstOrderBy = limitSpec.getColumns().get(0);

          if (firstOrderBy.getDimension().equals(dimensionSpec.getOutputName())) {
            // Order by time.
            descending = firstOrderBy.getDirection() == Direction.DESCENDING;
          } else {
            // Order by something else.
            return null;
          }
        }
      } else {
        // No limitSpec.
        descending = false;
      }
    } else {
      // More than one dimension, timeseries cannot handle.
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);
    final Map<String, Object> theContext = Maps.newHashMap();
    theContext.put("skipEmptyBuckets", true);
    theContext.putAll(plannerContext.getQueryContext());

    return new TimeseriesQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        queryGranularity,
        getVirtualColumns(),
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        null,
        null,
        null,
        null,
        ImmutableSortedMap.copyOf(theContext)
    );
  }

  /**
   * Return this query as a TopN query, or null if this query is not compatible with TopN.
   *
   * @return query or null
   */
  @Nullable
  public TopNQuery toTopNQuery()
  {
    if (grouping == null) {
      return null;
    }
    int maxTopNLimit = plannerContext.getPlannerConfig().getMaxTopNLimit();

    // Must have GROUP BY one column, ORDER BY zero or one column, limit less than maxTopNLimit, and no HAVING.
    List<DimensionExpression> dimensions = grouping.getDimensions();
    final boolean topNOk = dimensions.size() == 1 &&
                           limitSpec != null &&
                           limitSpec.getColumns().size() <= 1 && limitSpec.getLimit() <= maxTopNLimit &&
                           grouping.getHavingFilter() == null;

    if (!topNOk) {
      return null;
    }

    final DimensionExpression dimensionExpr = Iterables.getOnlyElement(dimensions);
    final DimensionSpec dimensionSpec = dimensionExpr.toDimensionSpec();
    if (!dimensionSpec.getDimension().equals(Column.TIME_COLUMN_NAME) &&
        !ValueDesc.isDimension(sourceRowSignature.getColumnType(dimensionSpec.getDimension()))) {
      return null;
    }
    final OrderByColumnSpec limitColumn;
    if (limitSpec.getColumns().isEmpty()) {
      limitColumn = new OrderByColumnSpec(
          dimensionSpec.getOutputName(),
          Direction.ASCENDING,
          Calcites.getStringComparatorForValueType(dimensionExpr.getOutputType())
      );
    } else {
      limitColumn = Iterables.getOnlyElement(limitSpec.getColumns());
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

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    return new TopNQuery(
        dataSource,
        getVirtualColumns(),
        dimensionSpec,
        topNMetricSpec,
        limitSpec.getLimit(),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a GroupBy query, or null if this query is not compatible with GroupBy.
   *
   * @return query or null
   */
  @Nullable
  public GroupByQuery toGroupByQuery()
  {
    if (grouping == null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    return new GroupByQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getDimensionSpecs(),
        ImmutableList.of(),
        getVirtualColumns(),
        grouping.getAggregatorFactories(),
        grouping.getPostAggregators(),
        grouping.getHavingFilter(),
        limitSpec,
        null,
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a Scan query, or null if this query is not compatible with Scan.
   *
   * @return query or null
   */
  @Nullable
  public StreamRawQuery toScanQuery()
  {
    if (grouping != null) {
      // Scan cannot GROUP BY.
      return null;
    }

    if (limitSpec != null && limitSpec.getColumns().size() > 0) {
      // Scan cannot ORDER BY.
      return null;
    }

    if (outputRowSignature.getRowOrder().isEmpty()) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("WTF?! Attempting to convert to Scan query without any columns?");
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);

    // LimitSpec (which we use to "remember" limits) is int typed, and Integer.MAX_VALUE means "no limit".
    final long scanLimit = limitSpec == null || limitSpec.getLimit() == Integer.MAX_VALUE
                           ? 0L
                           : (long) limitSpec.getLimit();

    List<String> columns = Ordering.natural().sortedCopy(ImmutableSet.copyOf(outputRowSignature.getRowOrder()));

    Preconditions.checkArgument(dataSource instanceof TableDataSource);

    ViewDataSource view = new ViewDataSource(
        ((TableDataSource)dataSource).getName(),
        columns,
        selectProjection != null ? selectProjection.getVirtualColumns() : null,
        filtration.getDimFilter(),
        false
    );
    return new StreamRawQuery(
        view,
        filtration.getQuerySegmentSpec(),
        null,
        Granularities.ALL,
        null,
        null,
        null,
        null,
        null,
        Ints.checkedCast(scanLimit),
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a Select query, or null if this query is not compatible with Select.
   *
   * @return query or null
   */
  @Nullable
  public SelectQuery toSelectQuery()
  {
    if (grouping != null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceRowSignature);
    final boolean descending;
    final int threshold;

    if (limitSpec != null) {
      // Safe to assume limitSpec has zero or one entry; DruidSelectSortRule wouldn't push in anything else.
      if (limitSpec.getColumns().size() == 0) {
        descending = false;
      } else if (limitSpec.getColumns().size() == 1) {
        final OrderByColumnSpec orderBy = Iterables.getOnlyElement(limitSpec.getColumns());
        if (!orderBy.getDimension().equals(Column.TIME_COLUMN_NAME)) {
          // Select cannot handle sorting on anything other than __time.
          return null;
        }
        descending = orderBy.getDirection() == Direction.DESCENDING;
      } else {
        // Select cannot handle sorting on more than one column.
        return null;
      }

      threshold = limitSpec.getLimit();
    } else {
      descending = false;
      threshold = 0;
    }

    // We need to ask for dummy columns to prevent Select from returning all of them.
    String dummyColumn = "dummy";
    while (sourceRowSignature.getColumnType(dummyColumn) != null
           || outputRowSignature.getRowOrder().contains(dummyColumn)) {
      dummyColumn = dummyColumn + "_";
    }

    final List<String> metrics = new ArrayList<>();

    if (selectProjection != null) {
      metrics.addAll(selectProjection.getDirectColumns());
      metrics.addAll(selectProjection.getVirtualColumns()
                                     .stream()
                                     .map(VirtualColumn::getOutputName)
                                     .collect(Collectors.toList()));
    } else {
      // No projection, rowOrder should reference direct columns.
      metrics.addAll(outputRowSignature.getRowOrder());
    }

    if (metrics.isEmpty()) {
      metrics.add(dummyColumn);
    }

    // Not used for actual queries (will be replaced by QueryMaker) but the threshold is important for the planner.
    final PagingSpec pagingSpec = new PagingSpec(null, threshold);

    return new SelectQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        Granularities.ALL,
        DefaultDimensionSpec.toSpec(dummyColumn),
        metrics.stream().sorted().distinct().collect(Collectors.toList()),
        getVirtualColumns(),
        pagingSpec,
        null,
        null,
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }
}
