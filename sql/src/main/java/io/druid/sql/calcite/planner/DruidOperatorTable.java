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

package io.druid.sql.calcite.planner;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.Utils;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.AvgSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MaxSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MinSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumZeroSqlAggregator;
import io.druid.sql.calcite.expression.AliasedOperatorConversion;
import io.druid.sql.calcite.expression.BinaryOperatorConversion;
import io.druid.sql.calcite.expression.DirectOperatorConversion;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.expression.UnaryPrefixOperatorConversion;
import io.druid.sql.calcite.expression.builtin.BTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CastOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CeilOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ConcatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.FloorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.LTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.MillisToTimestampOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ReinterpretOperatorConversion;
import io.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import io.druid.sql.calcite.expression.builtin.SubstringOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TextcatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeArithmeticOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeFormatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeParseOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeShiftOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimestampToMillisOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TruncateOperatorConversion;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidOperatorTable implements SqlOperatorTable
{
  private static final Logger LOG = new Logger(DruidOperatorTable.class);

  private static final List<SqlAggregator> STANDARD_AGGREGATORS =
      ImmutableList.<SqlAggregator>builder()
          .add(new ApproxCountDistinctSqlAggregator())
          .add(new AvgSqlAggregator())
          .add(new CountSqlAggregator())
          .add(new MinSqlAggregator())
          .add(new MaxSqlAggregator())
          .add(new SumSqlAggregator())
          .add(new SumZeroSqlAggregator())
          .build();

  // STRLEN has so many aliases.
  private static final SqlOperatorConversion CHARACTER_LENGTH_CONVERSION = new DirectOperatorConversion(
      SqlStdOperatorTable.CHARACTER_LENGTH,
      "strlen"
  );

  private static final List<SqlOperatorConversion> STANDARD_OPERATOR_CONVERSIONS =
      ImmutableList.<SqlOperatorConversion>builder()
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ABS, "abs"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CASE, "case"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHAR_LENGTH, "strlen"))
          .add(CHARACTER_LENGTH_CONVERSION)
          .add(new AliasedOperatorConversion(CHARACTER_LENGTH_CONVERSION, "LENGTH"))
          .add(new AliasedOperatorConversion(CHARACTER_LENGTH_CONVERSION, "STRLEN"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CONCAT, "concat"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.EXP, "exp"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.DIVIDE_INTEGER, "div"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LIKE, "like"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LN, "log"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOWER, "lower"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOG10, "log10"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.POWER, "pow"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.REPLACE, "replace"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SQRT, "sqrt"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.UPPER, "upper"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_NULL, "isNull"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "isNotNull"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_FALSE, "isFalse"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_NOT_TRUE, "isFalse"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_TRUE, "isTrue"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.IS_NOT_FALSE, "isTrue"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.NOT, "!"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.UNARY_MINUS, "-"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MULTIPLY, "*"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MOD, "%"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.DIVIDE, "/"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.PLUS, "+"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MINUS, "-"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.EQUALS, "=="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.NOT_EQUALS, "!="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN, ">"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN, "<"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.AND, "&&"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.OR, "||"))
          .add(new CastOperatorConversion())
          .add(new CeilOperatorConversion())
          .add(new DateTruncOperatorConversion())
          .add(new ExtractOperatorConversion())
          .add(new FloorOperatorConversion())
          .add(new MillisToTimestampOperatorConversion())
          .add(new ReinterpretOperatorConversion())
          .add(new RegexpExtractOperatorConversion())
          .add(new StrposOperatorConversion())
          .add(new SubstringOperatorConversion())
          .add(new ConcatOperatorConversion())
          .add(new TextcatOperatorConversion())
          .add(new AliasedOperatorConversion(new SubstringOperatorConversion(), "SUBSTR"))
          .add(new TimeArithmeticOperatorConversion.TimeMinusIntervalOperatorConversion())
          .add(new TimeArithmeticOperatorConversion.TimePlusIntervalOperatorConversion())
          .add(new TimeExtractOperatorConversion())
          .add(new TimeFloorOperatorConversion())
          .add(new TimeFormatOperatorConversion())
          .add(new TimeParseOperatorConversion())
          .add(new TimeShiftOperatorConversion())
          .add(new TimestampToMillisOperatorConversion())
          .add(new TruncateOperatorConversion())
          .add(new TrimOperatorConversion())
          .add(new BTrimOperatorConversion())
          .add(new LTrimOperatorConversion())
          .add(new RTrimOperatorConversion())
          .add(new AliasedOperatorConversion(new TruncateOperatorConversion(), "TRUNC"))
          .build();

  // Operators that have no conversion, but are handled in the convertlet table, so they still need to exist.
  private static final Map<OperatorKey, SqlOperator> CONVERTLET_OPERATORS =
      DruidConvertletTable.knownOperators()
                          .stream()
                          .collect(Collectors.toMap(OperatorKey::of, java.util.function.Function.identity()));

  private final Map<OperatorKey, SqlAggregator> aggregators;
  private final Map<OperatorKey, SqlOperatorConversion> operatorConversions;

  @Inject
  public DruidOperatorTable(
      final Set<SqlAggregator> userAggregators,
      final Set<AggregatorFactory.WithName> userAggregatorFactories,
      final Set<SqlOperatorConversion> userOperatorConversions
  )
  {
    this.aggregators = new HashMap<>();
    this.operatorConversions = new HashMap<>();

    for (SqlAggregator aggregator : userAggregators) {
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction());
      if (aggregators.put(operatorKey, aggregator) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (SqlAggregator aggregator : STANDARD_AGGREGATORS) {
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction());

      // Don't complain if the name already exists; we allow standard operators to be overridden.
      aggregators.putIfAbsent(operatorKey, aggregator);
    }

    for (AggregatorFactory.WithName named : userAggregatorFactories) {
      final AggregatorFactory factory = named.getValue();
      if (!(factory instanceof AggregatorFactory.SQLSupport)) {
        continue;
      }
      final Class clazz = Optional.fromNullable(factory.finalizedType()).or(ValueDesc.UNKNOWN).asClass();
      final SqlReturnTypeInference retType = ReturnTypes.cascade(
          ReturnTypes.explicit(Utils.asRelDataType(clazz)), SqlTypeTransforms.TO_NULLABLE
      );
      final SqlAggFunction function = new DummyAggregatorFunction(named.getKey(), retType);
      final SqlAggregator aggregator = new DummySqlAggregator(function, (AggregatorFactory.SQLSupport) factory);
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction());
      aggregators.putIfAbsent(operatorKey, aggregator);
    }

    for (SqlOperatorConversion operatorConversion : userOperatorConversions) {
      final OperatorKey operatorKey = OperatorKey.of(operatorConversion.calciteOperator());
      if (aggregators.containsKey(operatorKey) || operatorConversions.put(operatorKey, operatorConversion) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (SqlOperatorConversion operatorConversion : STANDARD_OPERATOR_CONVERSIONS) {
      final OperatorKey operatorKey = OperatorKey.of(operatorConversion.calciteOperator());

      // Don't complain if the name already exists; we allow standard operators to be overridden.
      if (aggregators.containsKey(operatorKey)) {
        continue;
      }

      operatorConversions.putIfAbsent(operatorKey, operatorConversion);
    }

    for (final Function.Factory factory : Parser.getAllFunctions()) {
      SqlReturnTypeInference retType;
      if (factory instanceof Function.TypeFixed) {
        ValueDesc type = factory.returns(null, null);
        if (type == null || type.asClass() == Object.class) {
          continue;
        }
        retType = ReturnTypes.explicit(Utils.asRelDataType(type.asClass()));
      } else {
        retType = new SqlReturnTypeInference()
        {
          @Override
          public RelDataType inferReturnType(SqlOperatorBinding opBinding)
          {
            List<Expr> operands = Lists.newArrayList();
            Map<String, ValueDesc> binding = Maps.newHashMap();
            for (int i = 0; i < opBinding.getOperandCount(); i++) {
              final ValueDesc type = Calcites.getValueDescForRelDataType(opBinding.getOperandType(i));
              if (opBinding.isOperandNull(i, false) || opBinding.isOperandLiteral(i, false)) {
                operands.add(Evals.constant(opBinding.getOperandLiteralValue(i, Object.class), type));
              } else {
                String identifier = "p" + i;
                operands.add(Evals.identifier(identifier));
                binding.put(identifier, type);
              }
            }
            ValueDesc type = factory.returns(operands, Parser.withTypeMap(binding));
            if (type != null && type.asClass() != null) {
              return Utils.asRelDataType(type.asClass());
            }
            return null;
          }
        };
      }

      String name = factory.name().toUpperCase();
      SqlOperator operator = new SqlFunction(
          name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(retType, SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.SYSTEM
      );

      OperatorKey operatorKey = OperatorKey.of(operator);
      if (!aggregators.containsKey(operatorKey)) {
        operatorConversions.putIfAbsent(operatorKey, new DirectOperatorConversion(operator, name));
      }
    }
  }

  public Aggregation lookupAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final SqlAggFunction aggregation = aggregateCall.getAggregation();
    final SqlAggregator sqlAggregator = aggregators.get(OperatorKey.of(aggregation));
    if (sqlAggregator != null) {
      return sqlAggregator.toDruidAggregation(
          plannerContext,
          rowSignature,
          rexBuilder,
          name,
          aggregateCall,
          project,
          existingAggregations,
          finalizeAggregations
      );
    } else {
      return null;
    }
  }

  public SqlOperatorConversion lookupOperatorConversion(final SqlOperator operator)
  {
    final OperatorKey operatorKey = OperatorKey.of(operator);
    final SqlOperatorConversion operatorConversion = operatorConversions.get(operatorKey);
    if (operatorConversion != null && operatorConversion.calciteOperator().equals(operator)) {
      return operatorConversion;
    } else {
      return null;
    }
  }

  @Override
  public void lookupOperatorOverloads(
      final SqlIdentifier opName,
      final SqlFunctionCategory category,
      final SqlSyntax syntax,
      final List<SqlOperator> operatorList
  )
  {
    if (opName.names.size() != 1) {
      return;
    }

    final OperatorKey operatorKey = OperatorKey.of(opName.getSimple(), syntax);

    final SqlAggregator aggregator = aggregators.get(operatorKey);
    if (aggregator != null) {
      operatorList.add(aggregator.calciteFunction());
    }

    final SqlOperatorConversion operatorConversion = operatorConversions.get(operatorKey);
    if (operatorConversion != null) {
      operatorList.add(operatorConversion.calciteOperator());
    }

    final SqlOperator convertletOperator = CONVERTLET_OPERATORS.get(operatorKey);
    if (convertletOperator != null) {
      operatorList.add(convertletOperator);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList()
  {
    final List<SqlOperator> retVal = new ArrayList<>();
    for (SqlAggregator aggregator : aggregators.values()) {
      retVal.add(aggregator.calciteFunction());
    }
    for (SqlOperatorConversion operatorConversion : operatorConversions.values()) {
      retVal.add(operatorConversion.calciteOperator());
    }
    retVal.addAll(DruidConvertletTable.knownOperators());
    return retVal;
  }

  private static SqlSyntax normalizeSyntax(final SqlSyntax syntax)
  {
    // Treat anything other than prefix/suffix/binary syntax as function syntax.
    if (syntax == SqlSyntax.PREFIX || syntax == SqlSyntax.BINARY || syntax == SqlSyntax.POSTFIX) {
      return syntax;
    } else {
      return SqlSyntax.FUNCTION;
    }
  }

  private static class OperatorKey
  {
    private final String name;
    private final SqlSyntax syntax;

    public OperatorKey(final String name, final SqlSyntax syntax)
    {
      this.name = StringUtils.toLowerCase(Preconditions.checkNotNull(name, "name"));
      this.syntax = normalizeSyntax(Preconditions.checkNotNull(syntax, "syntax"));
    }

    public static OperatorKey of(final String name, final SqlSyntax syntax)
    {
      return new OperatorKey(name, syntax);
    }

    public static OperatorKey of(final SqlOperator operator)
    {
      return new OperatorKey(operator.getName(), operator.getSyntax());
    }

    public String getName()
    {
      return name;
    }

    public SqlSyntax getSyntax()
    {
      return syntax;
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
      final OperatorKey that = (OperatorKey) o;
      return Objects.equals(name, that.name) &&
             syntax == that.syntax;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, syntax);
    }

    @Override
    public String toString()
    {
      return "OperatorKey{" +
             "name='" + name + '\'' +
             ", syntax=" + syntax +
             '}';
    }
  }

  private static class DummySqlAggregator implements SqlAggregator
  {
    private final SqlAggFunction function;
    private final AggregatorFactory.SQLSupport factory;

    private DummySqlAggregator(SqlAggFunction function, AggregatorFactory.SQLSupport factory)
    {
      this.function = function;
      this.factory = factory;
    }

    @Override
    public SqlAggFunction calciteFunction()
    {
      return function;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(
        PlannerContext plannerContext,
        RowSignature rowSignature,
        RexBuilder rexBuilder,
        String name,
        AggregateCall aggregateCall,
        Project project,
        List<Aggregation> existingAggregations,
        boolean finalizeAggregations
    )
    {
      if (aggregateCall.isDistinct()) {
        return null;
      }
      final List<DruidExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
          plannerContext,
          rowSignature,
          aggregateCall,
          project
      );
      if (arguments == null) {
        return null;
      }
      List<String> fieldNames = Lists.newArrayList();
      List<VirtualColumn> virtualColumns = Lists.newArrayList();
      for (DruidExpression expression : arguments) {
        if (expression.isDirectColumnAccess()) {
          fieldNames.add(expression.getDirectColumn());
        } else {
          ExprVirtualColumn virtualColumn = expression.toVirtualColumn(Calcites.makePrefixedName(name, "v"));
          fieldNames.add(virtualColumn.getOutputName());
          virtualColumns.add(virtualColumn);
        }
      }
      AggregatorFactory rewritten = factory.rewrite(name, fieldNames, rowSignature);
      if (rewritten == null) {
        return null;
      }
      if (finalizeAggregations) {
        return Aggregation.create(virtualColumns, ImmutableList.of(rewritten), AggregatorFactory.asFinalizer(rewritten));
      } else {
        return Aggregation.create(virtualColumns, rewritten);
      }
    }
  }

  private static class DummyAggregatorFunction extends SqlAggFunction
  {
    public DummyAggregatorFunction(String name, SqlReturnTypeInference retType)
    {
      super(
          name,
          null,
          SqlKind.OTHER_FUNCTION,
          retType,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.SYSTEM,
          false,
          false
      );
    }
  }
}
