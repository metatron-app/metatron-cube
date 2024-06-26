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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.common.guava.Sequence;
import io.druid.query.DataSource;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import javax.annotation.Nullable;
import java.util.function.Predicate;

public abstract class DruidRel extends AbstractRelNode
{
  public static RelOptRuleOperand anyDruid()
  {
    return operand(DruidRel.class);
  }

  static RelOptRuleOperand druid(Predicate<PartialDruidQuery> predicate)
  {
    return operand(DruidRel.class, druidRel -> predicate.test(druidRel.getPartialDruidQuery()));
  }

  public static <T extends RelNode> RelOptRuleOperand operand(Class<T> clazz)
  {
    return operand(clazz, r -> true);
  }

  public static <T extends RelNode> RelOptRuleOperand operand(Class<T> clazz, Predicate<T> predicate)
  {
    return RelOptRule.operandJ(clazz, null, predicate, RelOptRule.any());
  }

  public static <T extends RelNode> RelOptRuleOperand operand(
      Class<T> clazz, RelOptRuleOperand first, RelOptRuleOperand... rest
  )
  {
    return operand(clazz, r -> true, first, rest);
  }

  public static <T extends RelNode> RelOptRuleOperand operand(
      Class<T> clazz, Predicate<T> predicate, RelOptRuleOperand first, RelOptRuleOperand... rest
  )
  {
    return RelOptRule.operandJ(clazz, null, predicate, first, rest);
  }

  static final double COST_BASE = 1.0;

  public static interface LeafRel {
  }

  final QueryMaker queryMaker;

  protected DruidRel(RelOptCluster cluster, RelTraitSet traitSet, QueryMaker queryMaker)
  {
    super(cluster, traitSet);
    this.queryMaker = queryMaker;
  }

  /**
   * Returns the PartialDruidQuery associated with this DruidRel, and which can be built on top of. Returns null
   * if this rel cannot be built on top of.
   */
  @Nullable
  public PartialDruidQuery getPartialDruidQuery()
  {
    return null;
  }

  public DruidRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException("withPartialQuery");
  }

  public boolean hasFilter()
  {
    return false;
  }

  public RelNode getLeafRel()
  {
    if (this instanceof LeafRel) {
      return this;
    }
    PartialDruidQuery partialQuery = getPartialDruidQuery();
    return partialQuery == null ? null : partialQuery.leafRel();
  }

  public final Sequence<Object[]> runQuery()
  {
    // runQuery doesn't need to finalize aggregations, because the fact that runQuery is happening suggests this
    // is the outermost query and it will actually get run as a native query. Druid's native query layer will
    // finalize aggregations for the outermost query even if we don't explicitly ask it to.
    return queryMaker.prepareAndRun(toDruidQuery(false));
  }

  /**
   * Convert this DruidRel to a DruidQuery. This may be an expensive operation. For example, DruidSemiJoin needs to
   * execute the right-hand side query in order to complete this method.
   *
   * This method may return null if it knows that this rel will yield an empty result set.
   *
   * @param finalizeAggregations true if this query should include explicit finalization for all of its
   *                             aggregators, where required. Useful for subqueries where Druid's native query layer
   *                             does not do this automatically.
   *
   * @return query, or null if it is known in advance that this rel will yield an empty result set.
   *
   * @throws CannotBuildQueryException
   */
  @Nullable
  public abstract DruidQuery makeDruidQuery(boolean finalizeAggregations);

  final Supplier<DruidQuery> withFinalize = Suppliers.memoize(() -> makeDruidQuery(true));
  final Supplier<DruidQuery> withoutFinalize = Suppliers.memoize(() -> makeDruidQuery(false));

  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    return finalizeAggregations ? withFinalize.get() : withoutFinalize.get();
  }

  /**
   * Convert this DruidRel to a DruidQuery for purposes of explaining. This must be an inexpensive operation. For
   * example, DruidSemiJoin will use a dummy dataSource in order to complete this method, rather than executing
   * the right-hand side query.
   *
   * This method may not return null.
   *
   * @return query
   *
   * @throws CannotBuildQueryException
   */
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  public QueryMaker getQueryMaker()
  {
    return queryMaker;
  }

  public PlannerContext getPlannerContext()
  {
    return queryMaker.getPlannerContext();
  }

  public ObjectMapper getObjectMapper()
  {
    return queryMaker.getJsonMapper();
  }

  public abstract DruidRel asDruidConvention();

  /**
   * Get a list of names of datasources read by this DruidRel
   */
  public abstract DataSource getDataSource();
}
