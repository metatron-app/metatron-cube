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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidSpatialJoinRel extends DruidRel implements DruidRel.LeafRel
{
  private static final Logger LOG = new Logger(DruidSpatialJoinRel.class);

  public static DruidSpatialJoinRel create(
      DruidRel query,
      DruidRel boundary,
      RexNode queryOp,
      RexNode boundaryOp,
      boolean flip
  )
  {
    queryOp = query.getPartialDruidQuery().push(queryOp);
    boundaryOp = boundary.getPartialDruidQuery().push(boundaryOp);
    if (queryOp == null || boundaryOp == null) {
      return null;
    }
    LOG.debug(
        "%s (%s) + %s (%s) : %s",
        query.getDataSourceNames(), boundary.getDataSourceNames(),
        queryOp,
        boundaryOp,
        flip
    );
    return new DruidSpatialJoinRel(
        query.getCluster(),
        query.getTraitSet(),
        query,
        boundary,
        queryOp,
        boundaryOp,
        flip,
        query.getQueryMaker()
    );
  }

  private RelNode query;
  private RelNode boundary;
  private final RexNode queryOp;
  private final RexNode boundaryOp;
  private final boolean flip;

  private DruidSpatialJoinRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelNode query,
      final RelNode boundary,
      final RexNode queryOp,
      final RexNode boundaryOp,
      final boolean flip,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.query = query;
    this.boundary = boundary;
    this.queryOp = queryOp;
    this.boundaryOp = boundaryOp;
    this.flip = flip;
  }

  @Override
  public DruidSpatialJoinRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException("withPartialQuery");
  }

  @Override
  public DruidQuery makeDruidQuery(boolean finalizeAggregations)
  {
    final DruidQueryRel queryRel = Utils.findRel(query, DruidQueryRel.class);
    final DruidQueryRel boundaryRel = Utils.findRel(boundary, DruidQueryRel.class);
    if (queryRel == null || boundaryRel == null) {
      return null;
    }
    final DruidQuery query = queryRel.toDruidQuery(true);
    final DruidQuery boundary = boundaryRel.toDruidQuery(true);
    if (query == null || boundary == null) {
      return null;
    }
    PlannerContext context = getPlannerContext();
    RowSignature querySignature = query.getInputRowSignature();
    DruidExpression queryExpr = Expressions.toDruidExpression(context, querySignature, queryOp);
    if (queryExpr == null) {
      return null;
    }
    String queryColumn;
    Query sourceQuery = query.getQuery();
    if (queryExpr.isDirectColumnAccess()) {
      queryColumn = queryExpr.getDirectColumn();
    } else if (sourceQuery instanceof Query.VCSupport) {
      Query.VCSupport<?> vcSupport = (Query.VCSupport) sourceQuery;
      VirtualColumn vc = queryExpr.toVirtualColumn(queryColumn = "__vc");
      sourceQuery = vcSupport.withVirtualColumns(GuavaUtils.concat(vcSupport.getVirtualColumns(), vc));
    } else {
      return null;
    }
    RowSignature boundarySignature = boundary.getInputRowSignature();
    DruidExpression boundaryExpr = Expressions.toDruidExpression(context, boundarySignature, boundaryOp);
    if (boundaryExpr == null) {
      return null;
    }
    Query boundaryQuery = boundary.getQuery();
    RelDataType boundaryRows = boundary.getOutputRowType();
    String boundaryColumn;
    if (boundaryExpr.isDirectColumnAccess()) {
      boundaryColumn = boundaryExpr.getDirectColumn();
    } else if (boundaryQuery instanceof Query.VCSupport) {
      Query.VCSupport<?> vcSupport = (Query.VCSupport) boundaryQuery;
      VirtualColumn vc = boundaryExpr.toVirtualColumn(boundaryColumn = "__vc");
      boundaryQuery = vcSupport.withVirtualColumns(GuavaUtils.concat(vcSupport.getVirtualColumns(), vc));
    } else {
      return null;
    }
    final ObjectMapper mapper = getObjectMapper();
    final Map<String, Object> queryMap = Maps.newHashMap();
    queryMap.put("queryType", "geo.boundary");
    queryMap.put("query", mapper.convertValue(sourceQuery, Map.class));
    queryMap.put("queryColumn", queryColumn);
    queryMap.put("boundary", mapper.convertValue(boundaryQuery, Map.class));
    queryMap.put("boundaryColumn", boundaryColumn);
    queryMap.put("boundaryJoin", ImmutableList.copyOf(boundaryRows.getFieldNames()));
    queryMap.put("boundaryUnion", false);   // todo
    queryMap.put("flip", flip);
    queryMap.put("context", context.copyQueryContext());

    final Query.ArrayOutputSupport<?> converted = mapper.convertValue(queryMap, Query.ArrayOutputSupport.class);
    if (converted == null) {
      return null;
    }
    List<String> queryOrder = query.getOutputRowSignature().getColumnNames();
    List<String> boundaryOrder = boundary.getOutputRowSignature().getColumnNames();
    List<String> outputColumns = flip
                                 ? GuavaUtils.concat(boundaryOrder, queryOrder)
                                 : GuavaUtils.concat(queryOrder, boundaryOrder);
    final RowSignature signature = RowSignature.from(outputColumns, getRowType());

    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return getRowType();
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return signature;
      }

      @Override
      public Query getQuery()
      {
        return converted;
      }
    };
  }

  @Override
  public DruidSpatialJoinRel asDruidConvention()
  {
    return new DruidSpatialJoinRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        RelOptRule.convert(query, DruidConvention.instance()),
        RelOptRule.convert(boundary, DruidConvention.instance()),
        queryOp,
        boundaryOp,
        flip,
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    final DruidRel druidLeft = Utils.getDruidRel(query);
    final DruidRel druidRight = Utils.getDruidRel(boundary);
    Set<String> datasourceNames = new LinkedHashSet<>();
    datasourceNames.addAll(druidLeft.getDataSourceNames());
    datasourceNames.addAll(druidRight.getDataSourceNames());
    return new ArrayList<>(datasourceNames);
  }

  @Override
  public int getQueryCount()
  {
    DruidRel leftRel = Preconditions.checkNotNull(Utils.getDruidRel(query));
    DruidRel rightRel = Preconditions.checkNotNull(Utils.getDruidRel(boundary));
    return leftRel.getQueryCount() + rightRel.getQueryCount();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return SqlValidatorUtil.deriveJoinRowType(
        flip ? boundary.getRowType() : query.getRowType(),
        flip ? query.getRowType() : boundary.getRowType(),
        JoinRelType.INNER,
        getCluster().getTypeFactory(),
        null,
        ImmutableList.of()
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return ImmutableList.of(query, boundary);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    switch (ordinalInParent) {
      case 0:
        this.query = p;
        break;
      case 1:
        this.boundary = p;
        break;
      default:
        throw new IndexOutOfBoundsException("Input " + ordinalInParent);
    }
    recomputeDigest();
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidSpatialJoinRel(
        getCluster(),
        traitSet,
        inputs.get(0),
        inputs.get(1),
        queryOp,
        boundaryOp,
        flip,
        getQueryMaker()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw)
                .input("query", query)
                .input("boundary", boundary)
                .item("queryOp", queryOp)
                .item("boundaryOp", boundaryOp)
                .item("flip", flip);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    final RelOptCostFactory costFactory = planner.getCostFactory();
    final double lc = mq.getRowCount(query);
    final double rc = mq.getRowCount(boundary);
    return costFactory.makeCost(COST_BASE + lc + rc, 0, 0).multiplyBy(10);
  }
}
