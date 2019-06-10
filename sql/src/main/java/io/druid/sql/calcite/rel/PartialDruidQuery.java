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
  private final Filter whereFilter;
  private final Project selectProject;
  private final Sort selectSort;

  private final Aggregate aggregate;
  private final Project aggregateProject;   // mapped to PostAggregator
  private final Filter havingFilter;

  private final Window window;
  private final Sort sort;
  private final Project sortProject;        // mapped to PostAggregator with Sort, 'OutputColumns' with other

  public enum Stage
  {
    SCAN,
    WHERE_FILTER {
      @Override
      boolean accepts(Stage stage)
      {
        return stage == WHERE_FILTER;
      }
    },
    SELECT_PROJECT {
      @Override
      boolean accepts(Stage stage)
      {
        return stage == SELECT_PROJECT || stage == WHERE_FILTER;
      }
    },
    SELECT_SORT,
    AGGREGATE,
    HAVING_FILTER,
    AGGREGATE_PROJECT,
    WINDOW,
    SORT,
    SORT_PROJECT;

    boolean accepts(Stage stage)
    {
      return false;
    }
  }

  public PartialDruidQuery(
      final RelNode scan,
      final Filter whereFilter,
      final Project selectProject,
      final Sort selectSort,
      final Aggregate aggregate,
      final Project aggregateProject,
      final Filter havingFilter,
      final Window window,
      final Sort sort,
      final Project sortProject
  )
  {
    this.scan = Preconditions.checkNotNull(scan, "scan");
    this.whereFilter = whereFilter;
    this.selectProject = selectProject;
    this.selectSort = selectSort;
    this.aggregate = aggregate;
    this.aggregateProject = aggregateProject;
    this.havingFilter = havingFilter;
    this.window = window;
    this.sort = sort;
    this.sortProject = sortProject;
  }

  public static PartialDruidQuery create(final RelNode scanRel)
  {
    return new PartialDruidQuery(scanRel, null, null, null, null, null, null, null, null, null);
  }

  public RelNode getScan()
  {
    return scan;
  }

  public Filter getWhereFilter()
  {
    return whereFilter;
  }

  public Project getSelectProject()
  {
    return selectProject;
  }

  public Sort getSelectSort()
  {
    return selectSort;
  }

  public Aggregate getAggregate()
  {
    return aggregate;
  }

  public Filter getHavingFilter()
  {
    return havingFilter;
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

  public PartialDruidQuery withWhereFilter(final Filter newWhereFilter)
  {
    if (!canAccept(Stage.WHERE_FILTER)) {
      return null;
    }
    Filter merged;
    if (selectProject == null && whereFilter == null) {
      merged = newWhereFilter;
    } else {
      RexNode newCondition = newWhereFilter.getCondition();
      if (selectProject != null) {
        newCondition = RelOptUtil.pushPastProject(newCondition, selectProject);
      }
      RelNode input = whereFilter != null ? whereFilter.getInput() : selectProject.getInput();
      RelBuilder relBuilder = relBuilder().push(input);
      if (whereFilter == null) {
        relBuilder.filter(newCondition);
      } else {
        relBuilder.filter(whereFilter.getCondition(), newCondition);
      }
      merged = (Filter) relBuilder.build();
    }

    return new PartialDruidQuery(
        scan,
        merged,
        selectProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withSelectProject(final Project newSelectProject)
  {
    if (!canAccept(Stage.SELECT_PROJECT) || !checkUnsupported(newSelectProject)) {
      return null;
    }
    // Possibly merge together two projections.
    final Project theProject;
    if (selectProject == null) {
      theProject = newSelectProject;
    } else {
      final List<RexNode> newProjectRexNodes = RelOptUtil.pushPastProject(
          newSelectProject.getProjects(),
          selectProject
      );

      if (RexUtil.isIdentity(newProjectRexNodes, selectProject.getInput().getRowType())) {
        // The projection is gone.
        theProject = null;
      } else {
        RelBuilder relBuilder = relBuilder();
        relBuilder.push(selectProject.getInput());
        relBuilder.project(
            newProjectRexNodes,
            newSelectProject.getRowType().getFieldNames()
        );
        theProject = (Project) relBuilder.build();
      }
    }

    return new PartialDruidQuery(
        scan,
        whereFilter,
        theProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withSelectSort(final Sort newSelectSort)
  {
    if (newSelectSort == null) {
      return this;
    }
    if (!canAccept(Stage.SELECT_SORT) || !checkUnsupported(newSelectSort)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        newSelectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withAggregate(final Aggregate newAggregate)
  {
    if (!canAccept(Stage.AGGREGATE)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        newAggregate,
        aggregateProject,
        havingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withHavingFilter(final Filter newHavingFilter)
  {
    if (!canAccept(Stage.HAVING_FILTER)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        aggregateProject,
        newHavingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withAggregateProject(final Project newAggregateProject)
  {
    if (!canAccept(Stage.AGGREGATE_PROJECT) || !checkUnsupported(newAggregateProject)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        newAggregateProject,
        havingFilter,
        window,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withWindow(final Window newWindow)
  {
    if (!canAccept(Stage.WINDOW)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        newWindow,
        sort,
        sortProject
    );
  }

  public PartialDruidQuery withSort(final Sort newSort)
  {
    if (!canAccept(Stage.SORT) || !checkUnsupported(newSort)) {
      return null;
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        window,
        newSort,
        sortProject
    );
  }

  public PartialDruidQuery withSortProject(final Project newSortProject)
  {
    if (!canAccept(Stage.SORT_PROJECT) || !checkUnsupported(newSortProject)) {
      return null;
    }
    if (aggregate == null && !Utils.isAllInputRef(newSortProject.getChildExps())) {
      return null;    // stream query does not have post aggregation stage
    }
    return new PartialDruidQuery(
        scan,
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        aggregateProject,
        havingFilter,
        window,
        sort,
        newSortProject
    );
  }

  private boolean checkUnsupported(Project project)
  {
    for (RexNode rexNode : project.getProjects()) {
      if (rexNode instanceof RexOver || rexNode instanceof RexSubQuery) {
        return false;
      }
    }
    return true;
  }

  private boolean checkUnsupported(Sort sort)
  {
    for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
      if (collation.getDirection() != RelFieldCollation.Direction.ASCENDING &&
          collation.getDirection() != RelFieldCollation.Direction.DESCENDING) {
        return false;
      }
    }
    return true;
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

  public boolean canAccept(final Stage stage)
  {
//    final Stage current = stage();
//    final boolean accept = canAccept(current, stage);
//    LOG.info("-------- %s -> %s : %s", current, stage, accept ? "o" : "x");
//    return accept;
    return canAccept(stage(), stage);
  }

  private boolean canAccept(Stage current, Stage target)
  {
    if (current.accepts(target)) {
      return true;
    } else if (target.compareTo(current) <= 0) {
      // Cannot go backwards.
      return false;
    } else if (target.compareTo(Stage.SORT) > 0 && sort == null) {
      // Cannot add sort project without a sort
      return false;
    } else if (target.compareTo(Stage.AGGREGATE) >= 0 && selectSort != null) {
      // Cannot do any aggregations after a select + sort.
      return false;
    } else if (target != Stage.WINDOW && target.compareTo(Stage.AGGREGATE) > 0 && aggregate == null) {
      // Cannot do post-aggregation stages without an aggregation.
      return false;
    } else {
      // Looks good.
      return true;
    }
  }

  /**
   * Returns the stage corresponding to the rel at the end of the query. It will match the rel returned from
   * {@link #leafRel()}.
   *
   * @return stage
   */
  @SuppressWarnings("VariableNotUsedInsideIf")
  public Stage stage()
  {
    if (sortProject != null) {
      return Stage.SORT_PROJECT;
    } else if (sort != null) {
      return Stage.SORT;
    } else if (window != null) {
      return Stage.WINDOW;
    } else if (aggregateProject != null) {
      return Stage.AGGREGATE_PROJECT;
    } else if (havingFilter != null) {
      return Stage.HAVING_FILTER;
    } else if (aggregate != null) {
      return Stage.AGGREGATE;
    } else if (selectSort != null) {
      return Stage.SELECT_SORT;
    } else if (selectProject != null) {
      return Stage.SELECT_PROJECT;
    } else if (whereFilter != null) {
      return Stage.WHERE_FILTER;
    } else {
      return Stage.SCAN;
    }
  }

  /**
   * Returns the rel at the end of the query. It will match the stage returned from {@link #stage()}.
   *
   * @return leaf rel
   */
  public RelNode leafRel()
  {
    final Stage currentStage = stage();

    switch (currentStage) {
      case SORT_PROJECT:
        return sortProject;
      case SORT:
        return sort;
      case WINDOW:
        return window;
      case AGGREGATE_PROJECT:
        return aggregateProject;
      case HAVING_FILTER:
        return havingFilter;
      case AGGREGATE:
        return aggregate;
      case SELECT_SORT:
        return selectSort;
      case SELECT_PROJECT:
        return selectProject;
      case WHERE_FILTER:
        return whereFilter;
      case SCAN:
        return scan;
      default:
        throw new ISE("Unknown stage: %s", currentStage);
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
           Objects.equals(whereFilter, that.whereFilter) &&
           Objects.equals(selectProject, that.selectProject) &&
           Objects.equals(selectSort, that.selectSort) &&
           Objects.equals(aggregate, that.aggregate) &&
           Objects.equals(havingFilter, that.havingFilter) &&
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
        whereFilter,
        selectProject,
        selectSort,
        aggregate,
        havingFilter,
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
           ", whereFilter=" + whereFilter +
           ", selectProject=" + selectProject +
           ", selectSort=" + selectSort +
           ", aggregate=" + aggregate +
           ", havingFilter=" + havingFilter +
           ", aggregateProject=" + aggregateProject +
           ", window=" + window +
           ", sort=" + sort +
           ", sortProject=" + sortProject +
           '}';
  }
}
