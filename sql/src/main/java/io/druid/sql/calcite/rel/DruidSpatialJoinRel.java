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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.interpreter.BindableConvention;
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
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidSpatialJoinRel extends DruidRel<DruidSpatialJoinRel> implements DruidRel.LeafRel
{
  private static final Logger LOG = new Logger(DruidSpatialJoinRel.class);

  public static DruidSpatialJoinRel create(
      DruidRel query,
      DruidRel boundary,
      int queryIx,
      int boundaryIx,
      boolean flip
  )
  {
    LOG.debug(
        "%s (%s) + %s (%s) : %s",
        query.getDataSourceNames(), boundary.getDataSourceNames(),
        query.getRowType().getFieldList().get(queryIx),
        boundary.getRowType().getFieldList().get(boundaryIx),
        flip
    );
    return new DruidSpatialJoinRel(
        query.getCluster(),
        query.getTraitSet(),
        query,
        queryIx,
        boundary,
        boundaryIx,
        flip,
        query.getQueryMaker()
    );
  }

  private RelNode query;
  private RelNode boundary;
  private final int queryIx;
  private final int boundaryIx;
  private final boolean flip;

  private DruidSpatialJoinRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelNode query,
      final int queryIx,
      final RelNode boundary,
      final int boundaryIx,
      final boolean flip,
      final QueryMaker queryMaker
  )
  {
    super(cluster, traitSet, queryMaker);
    this.query = query;
    this.queryIx = queryIx;
    this.boundary = boundary;
    this.boundaryIx = boundaryIx;
    this.flip = flip;
  }

  @Override
  public DruidSpatialJoinRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException("withPartialQuery");
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    final RelDataType rowType = getRowType();
    final DruidRel queryRel = Utils.getDruidRel(query);
    final DruidRel boundaryRel = Utils.getDruidRel(boundary);
    if (queryRel == null || boundaryRel == null) {
      return null;
    }
    final DruidQuery query = queryRel.toDruidQuery(true);
    final DruidQuery boundary = boundaryRel.toDruidQuery(true);
    if (query == null || boundary == null) {
      return null;
    }
    final List<String> queryOrder = query.getOutputRowSignature().getRowOrder();
    final List<String> boundaryOrder = boundary.getOutputRowSignature().getRowOrder();

    final String queryColumn = queryOrder.get(queryIx);
    final String boundaryColumn = boundaryOrder.get(boundaryIx);
    final List<String> rowOrder =
        flip ? Queries.uniqueNames(boundaryOrder, queryOrder) : Queries.uniqueNames(queryOrder, boundaryOrder);
    final RowSignature outRowSignature = RowSignature.from(rowOrder, rowType);

    final ObjectMapper mapper = getObjectMapper();
    final Map<String, Object> queryMap = Maps.newHashMap();
    queryMap.put("queryType", "geo.boundary");
    queryMap.put("query", mapper.convertValue(query.getQuery(), Map.class));
    queryMap.put("queryColumn", queryColumn);
    queryMap.put("boundary", mapper.convertValue(boundary.getQuery(), Map.class));
    queryMap.put("boundaryColumn", boundaryColumn);
    queryMap.put("boundaryUnion", false);   // todo
    queryMap.put("flip", flip);

    List<String> boundaryJoin = Lists.newArrayList();
    boundaryJoin.add(boundaryColumn);
    for (int i = 0; i < boundaryOrder.size(); i++) {
      if (i != boundaryIx) {
        boundaryJoin.add(boundaryOrder.get(i));
      }
    }
    queryMap.put("boundaryJoin", boundaryJoin);

    final Query.ArrayOutputSupport<?> converted = mapper.convertValue(queryMap, Query.ArrayOutputSupport.class);
    if (converted == null) {
      return null;
    }
    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return rowType;
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return outRowSignature;
      }

      @Override
      public Query getQuery()
      {
        return converted;
      }
    };
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return toDruidQuery(false);
  }

  @Override
  public DruidSpatialJoinRel asBindable()
  {
    return new DruidSpatialJoinRel(
        getCluster(),
        getTraitSet().replace(BindableConvention.INSTANCE),
        RelOptRule.convert(query, BindableConvention.INSTANCE),
        queryIx,
        RelOptRule.convert(boundary, BindableConvention.INSTANCE),
        boundaryIx,
        flip,
        getQueryMaker()
    );
  }

  @Override
  public DruidSpatialJoinRel asDruidConvention()
  {
    return new DruidSpatialJoinRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        RelOptRule.convert(query, DruidConvention.instance()),
        queryIx,
        RelOptRule.convert(boundary, DruidConvention.instance()),
        boundaryIx,
        flip,
        getQueryMaker()
    );
  }

  @Override
  public List<String> getDataSourceNames()
  {
    final DruidRel<?> druidLeft = (DruidRel) query;
    final DruidRel<?> druidRight = (DruidRel) boundary;
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
        queryIx,
        inputs.get(1),
        boundaryIx,
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
                .item("queryIx", queryIx)
                .item("boundaryIx", boundaryIx)
                .item("flip", flip);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    final RelOptCostFactory costFactory = planner.getCostFactory();
    final double lc = mq.getRowCount(query);
    final double rc = mq.getRowCount(boundary);
    return costFactory.makeCost(COST_BASE + lc + rc, 0, 0).multiplyBy(10);
  }
}
