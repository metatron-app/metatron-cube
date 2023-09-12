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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DataSource;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for a Druid query, not counting the "dataSource" (which will be slotted in later).
 */
public class PartialDruidQuery
{
  private static final Logger LOG = new Logger(PartialDruidQuery.class);

  private final RelNode scan;
  private final PartialDruidQuery tableFunction;

  private final Filter scanFilter;
  private final Project scanProject;

  private final Aggregate aggregate;
  private final Filter aggregateFilter;
  private final Project aggregateProject;   // mapped to PostAggregator

  private final Window window;
  private final Sort sort;
  private final Project sortProject;        // mapped to PostAggregator with Sort, 'OutputColumns' with other

  private final PlannerContext context;

  public RexNode push(RexNode rexNode)
  {
    if (sortProject != null) {
      rexNode = RelOptUtil.pushPastProject(rexNode, sortProject);
    }
    if (window != null) {
      return null;  // todo
    }
    if (aggregateProject != null) {
      rexNode = RelOptUtil.pushPastProject(rexNode, aggregateProject);
    }
    if (aggregate != null) {
      int rex = Utils.soleInputRef(rexNode);
      if (rex < 0 || !aggregate.getGroupSet().get(rex)) {
        return null;
      }
    }
    if (scanProject != null) {
      rexNode = RelOptUtil.pushPastProject(rexNode, scanProject);
    }
    return rexNode;
  }

  public RelWriter explainTerms(RelWriter relWriter)
  {
    if (scanFilter != null) {
      relWriter.item("scanFilter", scanFilter.getCondition());
    }
    if (scanProject != null) {
      relWriter.item("scanProject", StringUtils.join(scanProject.getProjects(), ", "));
    }
    if (aggregate != null) {
      relWriter.itemIf("group", aggregate.getGroupSet(), !aggregate.getGroupSet().isEmpty())
               .itemIf("groups", aggregate.getGroupSets(), aggregate.getGroupType() != Aggregate.Group.SIMPLE);
      for (Ord<AggregateCall> ord : Ord.zip(aggregate.getAggCallList())) {
        relWriter.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    if (aggregateFilter != null) {
      relWriter.item("aggregateFilter", aggregateFilter.getCondition());
    }
    if (aggregateProject != null) {
      relWriter.item("aggregateProject", StringUtils.join(aggregateProject.getProjects(), ", "));
    }
    if (window != null) {
      for (Ord<Window.Group> window : Ord.zip(window.groups)) {
        relWriter.item("window#" + window.i, window.e.toString());
      }
    }
    if (sort != null) {
      final List<RexNode> childExps = sort.getSortExps();
      final List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
      final StringBuilder builder = new StringBuilder();
      for (int i = 0; i < childExps.size(); i++) {
        if (builder.length() > 0) {
          builder.append(", ");
        }
        builder.append(childExps.get(i));
        builder.append(':').append(collations.get(i).shortString());
      }
      relWriter.itemIf("sort", builder.toString(), builder.length() > 0);
      relWriter.itemIf("offset", sort.offset, sort.offset != null);
      relWriter.itemIf("fetch", sort.fetch, sort.fetch != null);
    }
    if (sortProject != null) {
      relWriter.item("sortProject", StringUtils.join(sortProject.getProjects(), ", "));
    }
    return relWriter;
  }

  public boolean hasHaving()
  {
    return aggregate != null && aggregateFilter != null;
  }

  public enum Operator
  {
    SCAN,
    FILTER,
    PROJECT,
    AGGREGATE,
    WINDOW,
    SORT
  }

  public enum Stage
  {
    SELECT,
    SELECT_SORT,
    SELECT_WINDOW,
    SELECT_WINDOW_SORT,
    AGGREGATE,
    AGGREGATE_SORT,
    AGGREGATE_WINDOW,
    AGGREGATE_WINDOW_SORT,
  }

  public PartialDruidQuery(
      final RelNode scan,
      final PartialDruidQuery tableFunction,
      final Filter scanFilter,
      final Project scanProject,
      final Aggregate aggregate,
      final Filter aggregateFilter,
      final Project aggregateProject,
      final Window window,
      final Sort sort,
      final Project sortProject,
      final PlannerContext context
  )
  {
    this.scan = Preconditions.checkNotNull(scan, "scan");
    this.tableFunction = tableFunction;
    this.scanFilter = scanFilter;
    this.scanProject = scanProject;
    this.aggregate = aggregate;
    this.aggregateProject = aggregateProject;
    this.aggregateFilter = aggregateFilter;
    this.window = window;
    this.sort = sort;
    this.sortProject = sortProject;
    this.context = context;
  }

  public static PartialDruidQuery create(RelNode scanRel, PlannerContext context)
  {
    return new PartialDruidQuery(scanRel, null, null, null, null, null, null, null, null, null, context);
  }

  public static PartialDruidQuery create(DruidRel inner)
  {
    return new PartialDruidQuery(
        inner.getLeafRel(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        inner.getPlannerContext()
    );
  }

  public RelNode getScan()
  {
    return scan;
  }

  public PartialDruidQuery getTableFunction()
  {
    return tableFunction;
  }

  public Filter getScanFilter()
  {
    return scanFilter;
  }

  public Project getScanProject()
  {
    return scanProject;
  }

  public Aggregate getAggregate()
  {
    return aggregate;
  }

  public Filter getAggregateFilter()
  {
    return aggregateFilter;
  }

  public Project getAggregateProject()
  {
    return aggregateProject;
  }

  public Window getWindow()
  {
    return window;
  }

  public Sort getSort()
  {
    return sort;
  }

  public Project getSortProject()
  {
    return sortProject;
  }

  public boolean isScanOnly()
  {
    return scanFilter == null && scanProject == null && stage() == Stage.SELECT;
  }

  public boolean isFilterScan()
  {
    return scanProject == null && stage() == Stage.SELECT;
  }

  public boolean isProjectScan()
  {
    return scanFilter == null && stage() == Stage.SELECT;
  }

  private RexBuilder rexBuilder()
  {
    return scan.getCluster().getRexBuilder();
  }

  private RelBuilder relBuilder()
  {
    return RelFactories.LOGICAL_BUILDER.create(
        scan.getCluster(),
        scan.getTable() == null ? null : scan.getTable().getRelOptSchema()
    );
  }

  public PartialDruidQuery withScan(RelNode scan)
  {
    return new PartialDruidQuery(
        scan,
        tableFunction,
        scanFilter,
        scanProject,
        aggregate,
        aggregateFilter,
        aggregateProject,
        window,
        sort,
        sortProject,
        context
    );
  }

  public PartialDruidQuery withTableFunction(PartialDruidQuery tableFunction)
  {
    return new PartialDruidQuery(
        scan,
        tableFunction,
        scanFilter,
        scanProject,
        aggregate,
        aggregateFilter,
        aggregateProject,
        window,
        sort,
        sortProject,
        context
    );
  }

  public PartialDruidQuery mergeScanFilter(Filter filter)
  {
    RelBuilder relBuilder = relBuilder().push(scan);
    if (scanFilter == null) {
      relBuilder.filter(filter.getCondition());
    } else {
      relBuilder.filter(scanFilter.getCondition(), filter.getCondition());
    }
    return new PartialDruidQuery(
        scan,
        tableFunction,
        (Filter) relBuilder.build(),
        scanProject,
        aggregate,
        aggregateFilter,
        aggregateProject,
        window,
        sort,
        sortProject,
        context
    );
  }

  public PartialDruidQuery withFilter(final Filter newFilter)
  {
    switch (stage()) {
      case SELECT:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            mergeFilter(newFilter, scanFilter, scanProject),
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject,
            context
        );
      case AGGREGATE:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            aggregate,
            mergeFilter(newFilter, aggregateFilter, aggregateProject),
            aggregateProject,
            window,
            sort,
            sortProject,
            context
        );
      default:
        return null;
    }
  }

  private Filter mergeFilter(Filter newFilter, Filter current, Project project)
  {
    if (project == null && current == null) {
      return newFilter;
    }
    RexNode newCondition = newFilter.getCondition();
    if (project != null) {
      newCondition = RelOptUtil.pushPastProject(newCondition, project);
    }
    RelNode input = current != null ? current.getInput() : project.getInput();
    RelBuilder relBuilder = relBuilder().push(input);
    if (current == null) {
      relBuilder.filter(newCondition);
    } else {
      relBuilder.filter(current.getCondition(), newCondition);
    }
    return (Filter) relBuilder.build();
  }

  public PartialDruidQuery withProject(final Project newProject)
  {
    if (!supports(newProject)) {
      return null;
    }
    switch (stage()) {
      case SELECT:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            mergeProject(newProject, scanProject),
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject,
            context
        );
      case AGGREGATE:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            mergeProject(newProject, aggregateProject),
            window,
            sort,
            sortProject,
            context
        );
      case SELECT_SORT:
      case SELECT_WINDOW:
      case SELECT_WINDOW_SORT:
        if (!Utils.isAllInputRefs(newProject)) {
          return null;
        }
        // break through
      case AGGREGATE_SORT:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            mergeProject(newProject, sortProject),
            context
        );
      default:
        return null;
    }
  }

  private Project mergeProject(Project newProject, Project current)
  {
    final Project theProject;
    if (current == null) {
      return newProject;
    }
    final List<RexNode> newProjectRexNodes = RelOptUtil.pushPastProject(
        newProject.getProjects(),
        current
    );
    if (RexUtil.isIdentity(newProjectRexNodes, current.getInput().getRowType())) {
      // The projection is gone.
      return null;
    }
    RelBuilder relBuilder = relBuilder();
    relBuilder.push(current.getInput());
    relBuilder.project(
        newProjectRexNodes,
        newProject.getRowType().getFieldNames()
    );
    return (Project) relBuilder.build();
  }

  public PartialDruidQuery withScanProject(RelNode source, Filter scanFilter, Project scanProject)
  {
    return new PartialDruidQuery(
        source,
        tableFunction,
        scanFilter,
        scanProject,
        aggregate,
        aggregateFilter,
        aggregateProject,
        window,
        sort,
        sortProject,
        context
    );
  }

  public PartialDruidQuery withAggregate(final Aggregate newAggregate)
  {
    if (!context.getPlannerConfig().isUseApproximateCountDistinct()) {
      for (AggregateCall call : newAggregate.getAggCallList()) {
        if (call.getAggregation().kind == SqlKind.COUNT && call.isDistinct()) {
          return null;
        }
      }
    }
    switch (stage()) {
      case SELECT:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            newAggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject,
            context
        );
      default:
        return null;
    }
  }

  public PartialDruidQuery withWindow(final Window newWindow)
  {
    switch (stage()) {
      case SELECT:
      case AGGREGATE:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            newWindow,
            sort,
            sortProject,
            context
        );
      default:
        return null;
    }
  }

  public PartialDruidQuery withSort(final Sort newSort)
  {
    if (!supports(newSort)) {
      return null;
    }
    switch (stage()) {
      case SELECT:
      case SELECT_WINDOW:
      case AGGREGATE:
      case AGGREGATE_WINDOW:
        return new PartialDruidQuery(
            scan,
            tableFunction,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            newSort,
            sortProject,
            context
        );
      default:
        return null;
    }
  }

  public PartialDruidQuery rewrite(RexShuttle shuttle)
  {
    if (shuttle == null) {
      return this;
    }
    // todo: try simplify rex
    return new PartialDruidQuery(
        scan,
        tableFunction == null ? null : tableFunction.rewrite(shuttle),
        Utils.apply(scanFilter, shuttle),
        Utils.apply(scanProject, shuttle),
        Utils.apply(aggregate, shuttle),
        Utils.apply(aggregateFilter, shuttle),
        Utils.apply(aggregateProject, shuttle),
        Utils.apply(window, shuttle),
        Utils.apply(sort, shuttle),
        Utils.apply(sortProject, shuttle),
        context
    );
  }

  private boolean supports(Project project)
  {
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexSubQuery || RexOver.containsOver(project.getProjects(), null)) {
        return false;
      }
    }
    return true;
  }

  private boolean supports(Sort sort)
  {
    if (sort.offset != null || !Utils.isAllInputRefs(sort.getSortExps())) {
      return false;
    }
    for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
      if (collation.getDirection() != RelFieldCollation.Direction.ASCENDING &&
          collation.getDirection() != RelFieldCollation.Direction.DESCENDING) {
        return false;
      }
    }
    return true;
  }

  public boolean canAccept(Operator operator)
  {
    switch (stage()) {
      case SELECT:
      case AGGREGATE:
        return true;
      default:
        return operator == Operator.SORT || operator == Operator.PROJECT;
    }
  }

  public RelDataType getRowType()
  {
    return leafRel().getRowType();
  }

  public RelTrait getCollation()
  {
    return leafRel().getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
  }

  public DruidQuery build(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final Map<String, Object> contextOverride,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    return new DruidBaseQuery(
        rewrite(PlannerContext.PARAMETER_BINDING.get()),
        dataSource,
        sourceRowSignature,
        plannerContext,
        contextOverride,
        rexBuilder,
        finalizeAggregations
    );
  }

  /**
   * Returns the stage corresponding to the rel at the end of the query. It will match the rel returned from
   * {@link #leafRel()}.
   *
   * @return stage
   */
  public Stage stage()
  {
    if (aggregate == null) {
      if (window == null) {
        if (sortProject == null && sort == null) {
          return Stage.SELECT;
        } else {
          return Stage.SELECT_SORT;
        }
      } else {
        if (sortProject == null && sort == null) {
          return Stage.SELECT_WINDOW;
        } else {
          return Stage.SELECT_WINDOW_SORT;
        }
      }
    } else {
      if (window == null) {
        if (sortProject == null && sort == null) {
          return Stage.AGGREGATE;
        } else {
          return Stage.AGGREGATE_SORT;
        }
      } else {
        if (sortProject == null && sort == null) {
          return Stage.AGGREGATE_WINDOW;
        } else {
          return Stage.AGGREGATE_WINDOW_SORT;
        }
      }
    }
  }

  /**
   * Returns the rel at the end of the query. It will match the stage returned from {@link #stage()}.
   *
   * @return leaf rel
   */
  public RelNode leafRel()
  {
    switch (stage()) {
      case SELECT:
        return scanProject != null ? scanProject : scanFilter != null ? scanFilter : scan;
      case SELECT_WINDOW:
      case AGGREGATE_WINDOW:
        return window;
      case SELECT_SORT:
      case SELECT_WINDOW_SORT:
      case AGGREGATE_SORT:
      case AGGREGATE_WINDOW_SORT:
        return sortProject != null ? sortProject : sort;
      case AGGREGATE:
        return aggregateProject != null ? aggregateProject : aggregateFilter != null ? aggregateFilter : aggregate;
      default:
        throw new ISE("never.. %s", this);
    }
  }

  // Factors used for computing cost (see computeSelfCost). These are intended to encourage pushing down filters
  // and limits through stacks of nested queries when possible.
  private static final double PROJECT_BASE = 0.4;
  private static final double PROJECT_BASE_OUTER = 0.8;
  private static final double PROJECT_BASE_REMAIN = 0.9;

  private static final double AGGR_PER_COLUMN = 0.1;

  private static final double WINDOW_MULTIPLIER = 3.0;
  private static final double DIST_SORT_MULTIPLIER = 1.2;
  public static final double SORT_MULTIPLIER = 2.5;
  public static final double JOIN_MULTIPLIER = 4;

  public RelOptCost cost(DruidTable table, RelOptCostFactory factory)
  {
    return cost(table.getStatistic().getRowCount(), factory);
  }

  public RelOptCost cost(double base, RelOptCostFactory factory)
  {
    boolean tableScan = scan instanceof TableScan;
    double numColumns = scan.getRowType().getFieldCount();

    double cost = base;
    double estimate = base;

    if (scanFilter != null) {
      RexNode condition = scanFilter.getCondition();
      cost += estimate * Utils.rexEvalCost(condition);
      estimate *= Utils.selectivity(scanFilter, condition);
    }

    if (scanProject != null) {
      List<RexNode> rexNodes = scanProject.getProjects();
      cost += estimate * Utils.rexEvalCost(rexNodes);
      double ratio = tableScan ? PROJECT_BASE : PROJECT_BASE_OUTER;
      estimate *= ratio + (1 - ratio) * (rexNodes.size() / numColumns);
      numColumns = rexNodes.size();
    }

    if (aggregate != null) {
      int groupings = aggregate.getGroupSets().size();
      int dimensionality = aggregate.getGroupSet().cardinality();
      cost += estimate * Utils.aggregationCost(dimensionality, aggregate.getAggCallList()) * groupings;
      estimate *= Utils.aggregationRow(dimensionality) * groupings;
      numColumns += dimensionality;
    }

    if (aggregateProject != null) {
      List<RexNode> rexNodes = aggregateProject.getProjects();
      cost += estimate * Utils.rexEvalCost(rexNodes);
      estimate *= PROJECT_BASE_REMAIN + (1 - PROJECT_BASE_REMAIN) * (rexNodes.size() / numColumns);
      numColumns = rexNodes.size();
    }

    if (aggregateFilter != null) {
      RexNode condition = aggregateFilter.getCondition();
      cost += estimate * Utils.rexEvalCost(condition);
      estimate *= Utils.selectivity(aggregateFilter, condition);
    }

    if (window != null) {
      cost += estimate * WINDOW_MULTIPLIER * window.groups.size();
    }

    if (sort != null) {
      cost += estimate * (tableScan && aggregate == null && window == null ? DIST_SORT_MULTIPLIER : SORT_MULTIPLIER);
      final Integer limit = sort.fetch instanceof RexLiteral ? RexLiteral.intValue(sort.fetch) : null;
      if (limit != null) {
        estimate = Math.max(1, Math.min(estimate - 1, limit));
        cost -= 0.1;
      }
    }

    if (sortProject != null) {
      List<RexNode> rexNodes = sortProject.getProjects();
      cost += estimate * (0.0001 + Utils.rexEvalCost(rexNodes));
      estimate *= PROJECT_BASE_REMAIN + (1 - PROJECT_BASE_REMAIN) * (rexNodes.size() / numColumns);
    }

    return factory.makeCost(estimate, cost, 0);
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PartialDruidQuery that = (PartialDruidQuery) o;
    return Objects.equals(scan, that.scan) &&
           Objects.equals(scanFilter, that.scanFilter) &&
           Objects.equals(scanProject, that.scanProject) &&
           Objects.equals(aggregate, that.aggregate) &&
           Objects.equals(aggregateFilter, that.aggregateFilter) &&
           Objects.equals(aggregateProject, that.aggregateProject) &&
           Objects.equals(window, that.window) &&
           Objects.equals(sort, that.sort) &&
           Objects.equals(sortProject, that.sortProject);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        scan,
        scanFilter,
        scanProject,
        aggregate,
        aggregateFilter,
        aggregateProject,
        window,
        sort,
        sortProject
    );
  }

  @Override
  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    if (scan.getTable() != null) {
      builder.append("scan=").append(scan.getTable().getQualifiedName());
    }
    if (scanFilter != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("scanFilter=").append(scanFilter.getCondition());
    }
    if (scanProject != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("scanProject=").append(scanProject.getProjects());
    }
    if (aggregate != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("aggregate=").append(aggregate);
    }
    if (aggregateFilter != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("aggregateFilter=").append(aggregateFilter.getCondition());
    }
    if (aggregateProject != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("aggregateProject=").append(aggregateProject.getProjects());
    }
    if (window != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("window=").append(window);
    }
    if (sort != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("sort=").append(sort);
    }
    if (sortProject != null) {
      if (builder.length() > 0) builder.append(", ");
      builder.append("sortProject=").append(sortProject.getProjects());
    }
    return builder.toString();
  }
}
