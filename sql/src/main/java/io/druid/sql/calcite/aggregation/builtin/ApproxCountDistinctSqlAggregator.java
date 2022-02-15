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

package io.druid.sql.calcite.aggregation.builtin;

import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ApproxCountDistinctSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new ApproxCountDistinctSqlAggFunction();
  private static final String NAME = "APPROX_COUNT_DISTINCT";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final boolean finalizeAggregations
  )
  {
    // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
    // for string columns.
    final List<RexNode> rexNodes = Expressions.fromFieldAccesses(rowSignature, project, aggregateCall.getArgList());
    final List<DruidExpression> args = Expressions.toDruidExpressions(plannerContext, rowSignature, rexNodes);
    if (GuavaUtils.isNullOrEmpty(args)) {
      return null;
    }

    final List<VirtualColumn> virtualColumns = new ArrayList<>();
    final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;

    final DruidExpression first = args.get(0);
    final AggregatorFactory factory;
    if (args.size() == 1 && first.isDirectColumnAccess() &&
        HyperLogLogCollector.HLL_TYPE.equals(rowSignature.resolve(first.getDirectColumn()))) {
      factory = HyperUniquesAggregatorFactory.of(aggregatorName, first.getDirectColumn());
    } else {

      final List<DimensionSpec> dimensions = Lists.newArrayListWithCapacity(args.size());

      for (DruidExpression arg : args) {
        if (arg.isSimpleExtraction()) {
          dimensions.add(arg.getSimpleExtraction().toDimensionSpec(null));
        } else {
          String suffix = dimensions.isEmpty() ? "v" : "v" + dimensions.size();
          ExprVirtualColumn vc = arg.toVirtualColumn(Calcites.makePrefixedName(name, suffix));
          virtualColumns.add(vc);
          dimensions.add(DefaultDimensionSpec.of(vc.getOutputName()));
        }
      }

      factory = CardinalityAggregatorFactory.dimensions(aggregatorName, dimensions, GroupingSetSpec.EMPTY);
    }

    return Aggregation.create(
        rowSignature,
        virtualColumns,
        Collections.singletonList(factory),
        finalizeAggregations ? new HyperUniqueFinalizingPostAggregator(name, factory.getName(), true) : null
    );
  }

  private static class ApproxCountDistinctSqlAggFunction extends SqlAggFunction
  {
    private ApproxCountDistinctSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.BIGINT),
          InferTypes.VARCHAR_1024,
          OperandTypes.ANY,
          SqlFunctionCategory.STRING,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
