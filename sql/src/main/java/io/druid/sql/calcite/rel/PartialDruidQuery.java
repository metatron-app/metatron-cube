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

package io.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.query.DataSource;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Objects;

/**
 * Builder for a Druid query, not counting the "dataSource" (which will be slotted in later).
 */
public class PartialDruidQuery
{
  private static final Logger LOG = new Logger(PartialDruidQuery.class);

  private final RelNode scan;
  private final Filter scanFilter;
  private final Project scanProject;

  private final Aggregate aggregate;
  private final Filter aggregateFilter;
  private final Project aggregateProject;   // mapped to PostAggregator

  private final Window window;
  private final Sort sort;
  private final Project sortProject;        // mapped to PostAggregator with Sort, 'OutputColumns' with other

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
      final Filter scanFilter,
      final Project scanProject,
      final Aggregate aggregate,
      final Filter aggregateFilter,
      final Project aggregateProject,
      final Window window,
      final Sort sort,
      final Project sortProject
  )
  {
    this.scan = Preconditions.checkNotNull(scan, "scan");
    this.scanFilter = scanFilter;
    this.scanProject = scanProject;
    this.aggregate = aggregate;
    this.aggregateProject = aggregateProject;
    this.aggregateFilter = aggregateFilter;
    this.window = window;
    this.sort = sort;
    this.sortProject = sortProject;
  }

  public static PartialDruidQuery create(final RelNode scanRel)
  {
    return new PartialDruidQuery(scanRel, null, null, null, null, null, null, null, null);
  }

  public RelNode getScan()
  {
    return scan;
  }

  public Filter getScanFilter()
  {
    return scanFilter;
  }

  public Project getSelectProject()
  {
    return scanProject;
  }

  public Aggregate getAggregate()
  {
    return aggregate;
  }

  public Filter getHavingFilter()
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

  public PartialDruidQuery withFilter(final Filter newFilter)
  {
    switch (stage()) {
      case SELECT:
        return new PartialDruidQuery(
            scan,
            mergeFilter(newFilter, scanFilter, scanProject),
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject
        );
      case AGGREGATE:
        return new PartialDruidQuery(
            scan,
            scanFilter,
            scanProject,
            aggregate,
            mergeFilter(newFilter, aggregateFilter, aggregateProject),
            aggregateProject,
            window,
            sort,
            sortProject
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
            scanFilter,
            mergeProject(newProject, scanProject),
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject
        );
      case AGGREGATE:
        return new PartialDruidQuery(
            scan,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            mergeProject(newProject, aggregateProject),
            window,
            sort,
            sortProject
        );
      case SELECT_SORT:
      case SELECT_WINDOW:
      case SELECT_WINDOW_SORT:
        if (!Utils.isAllInputRef(newProject.getChildExps())) {
          return null;
        }
        // break through
      case AGGREGATE_SORT:
        return new PartialDruidQuery(
            scan,
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            mergeProject(newProject, sortProject)
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

  public PartialDruidQuery withAggregate(final Aggregate newAggregate)
  {
    switch (stage()) {
      case SELECT:
        return new PartialDruidQuery(
            scan,
            scanFilter,
            scanProject,
            newAggregate,
            aggregateFilter,
            aggregateProject,
            window,
            sort,
            sortProject
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
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            newWindow,
            sort,
            sortProject
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
            scanFilter,
            scanProject,
            aggregate,
            aggregateFilter,
            aggregateProject,
            window,
            newSort,
            sortProject
        );
      default:
        return null;
    }
  }

  private boolean supports(Project project)
  {
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexOver || rexNode instanceof RexSubQuery) {
        return false;
      }
    }
    return true;
  }

  private boolean supports(Sort sort)
  {
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

  public RelTrait[] getRelTraits()
  {
    return leafRel().getTraitSet().toArray(new RelTrait[0]);
  }

  public DruidQuery build(
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    return new DruidBaseQuery(this, dataSource, sourceRowSignature, plannerContext, rexBuilder, finalizeAggregations);
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
    return "PartialDruidQuery{" +
           "scan=" + scan +
           ", scanFilter=" + scanFilter +
           ", scanProject=" + scanProject +
           ", aggregate=" + aggregate +
           ", aggregateFilter=" + aggregateFilter +
           ", aggregateProject=" + aggregateProject +
           ", window=" + window +
           ", sort=" + sort +
           ", sortProject=" + sortProject +
           '}';
  }
}
