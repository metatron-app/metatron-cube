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
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.CombinedDataSource;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.UnionAllQuery;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidUnionRel extends DruidRel implements DruidRel.LeafRel
{
  private final RelDataType rowType;
  private final List<RelNode> rels;
  private final int limit;

  private DruidUnionRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final QueryMaker queryMaker,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    super(cluster, traitSet, queryMaker);
    this.rowType = rowType;
    this.rels = rels;
    this.limit = limit;
  }

  public static DruidUnionRel create(
      final QueryMaker queryMaker,
      final RelDataType rowType,
      final List<RelNode> rels,
      final int limit
  )
  {
    Preconditions.checkState(rels.size() > 0, "rels must be nonempty");

    return new DruidUnionRel(
        rels.get(0).getCluster(),
        rels.get(0).getTraitSet(),
        queryMaker,
        rowType,
        new ArrayList<>(rels),
        limit
    );
  }

  @Override
  public DruidQuery makeDruidQuery(final boolean finalizeAggregations)
  {
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    List<RelDataType> converted = GuavaUtils.transform(rowType.getFieldList(), RelDataTypeField::getType);
    RowSignature signature0 = null;
    List<Query> queries = Lists.newArrayList();
    for (RelNode relNode : rels) {
      DruidRel druidRel = Utils.getDruidRel(relNode);
      if (druidRel == null) {
        return null;
      }
      DruidQuery druidQuery = druidRel.toDruidQuery(finalizeAggregations);
      if (druidQuery == null) {
        return null;
      }
      queries.add(druidQuery.getQuery());

      RowSignature signature1 = druidQuery.getOutputRowSignature();
      if (signature0 == null) {
        signature0 = signature1;
      } else {
        Preconditions.checkArgument(
            Iterables.elementsEqual(signature0.getColumnTypes(), signature1.getColumnTypes()),
            "Inconsistent type %s with %s", signature0, signature1
        );
      }
      List<RelDataTypeField> fields = druidQuery.getOutputRowType().getFieldList();
      for (int i = 0; i < converted.size(); i++) {
        if (converted.get(i).getSqlTypeName() != SqlTypeName.ANY) {
          continue;
        }
        RelDataType type = fields.get(i).getType();
        if (converted.get(i).isNullable() && !type.isNullable()) {
          type = typeFactory.createTypeWithNullability(type, true);
        }
        converted.set(i, type);
      }
    }
    final RelDataType dataType = typeFactory.createStructType(converted, rowType.getFieldNames());
    final RowSignature signature = signature0;
    final Query query = UnionAllQuery.union(queries, limit, getPlannerContext().copyQueryContext())
                                     .withSchema(Suppliers.ofInstance(signature));
    return new DruidQuery()
    {
      @Override
      public RelDataType getOutputRowType()
      {
        return dataType;
      }

      @Override
      public RowSignature getOutputRowSignature()
      {
        return signature;
      }

      @Override
      public Query getQuery()
      {
        return query;
      }
    };
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public DruidUnionRel asDruidConvention()
  {
    return new DruidUnionRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        queryMaker,
        rowType,
        rels.stream().map(rel -> RelOptRule.convert(rel, DruidConvention.instance())).collect(Collectors.toList()),
        limit
    );
  }

  @Override
  public DataSource getDataSource()
  {
    return new CombinedDataSource(
        rels.stream().map(rel -> Utils.getDruidRel(rel).getDataSource()).collect(Collectors.toList())
    );
  }

  @Override
  public List<RelNode> getInputs()
  {
    return rels;
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p)
  {
    rels.set(ordinalInParent, p);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs)
  {
    return new DruidUnionRel(
        getCluster(),
        traitSet,
        queryMaker,
        rowType,
        inputs,
        limit
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    super.explainTerms(pw);

    for (int i = 0; i < rels.size(); i++) {
      pw.input("input#" + i, rels.get(i));
    }

    return pw.item("limit", limit);
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return rowType;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq, Set<RelNode> visited)
  {
    if (!visited.add(this)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    double rowCount = 0;
    double cpu = 0;
    for (RelNode sourceRel : rels) {
      final Pair<DruidRel, RelOptCost> m = Utils.getMinimumCost(sourceRel, planner, mq, visited);
      if (m.right == null || m.right.isInfinite()) {
        return planner.getCostFactory().makeInfiniteCost();
      }
      rowCount += m.right.getRows();
      cpu += m.right.getCpu();
    }
    return planner.getCostFactory().makeCost(rowCount, cpu, 0);
  }

  public int getLimit()
  {
    return limit;
  }
}
