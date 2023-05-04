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

package io.druid.sql.calcite.planner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import io.druid.math.expr.WindowFunctions;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators.RelayType;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.WindowContext;
import io.druid.sql.calcite.aggregation.Aggregations;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.AvgSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MaxSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.MinSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.RelayAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumSqlAggregator;
import io.druid.sql.calcite.aggregation.builtin.SumZeroSqlAggregator;
import io.druid.sql.calcite.expression.AliasedOperatorConversion;
import io.druid.sql.calcite.expression.BinaryOperatorConversion;
import io.druid.sql.calcite.expression.BitSetCardinalityConversion;
import io.druid.sql.calcite.expression.BitSetUnwrapConversion;
import io.druid.sql.calcite.expression.DimFilterConversion;
import io.druid.sql.calcite.expression.DirectOperatorConversion;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.NominalBitSetToStringConversion;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.expression.UnaryPrefixOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ArrayConstructorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.BTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CastOperatorConversion;
import io.druid.sql.calcite.expression.builtin.CeilOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ConcatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.DateTruncOperatorConversion;
import io.druid.sql.calcite.expression.builtin.DedupOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.FloorOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ItemOperatorConversion;
import io.druid.sql.calcite.expression.builtin.LPadOperatorConversion;
import io.druid.sql.calcite.expression.builtin.LTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.LeftOperatorConversion;
import io.druid.sql.calcite.expression.builtin.MillisToTimestampOperatorConversion;
import io.druid.sql.calcite.expression.builtin.NominalTypeConversion;
import io.druid.sql.calcite.expression.builtin.RPadOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RTrimOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RegexpExtractOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RegexpLikeOperatorConversion;
import io.druid.sql.calcite.expression.builtin.ReinterpretOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RepeatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RightOperatorConversion;
import io.druid.sql.calcite.expression.builtin.RoundOperatorConversion;
import io.druid.sql.calcite.expression.builtin.SizeOperatorConversion;
import io.druid.sql.calcite.expression.builtin.StringFormatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.StrposOperatorConversion;
import io.druid.sql.calcite.expression.builtin.SubstringOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TextcatOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeArithmeticOperatorConversion;
import io.druid.sql.calcite.expression.builtin.TimeCeilOperatorConversion;
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Optionality;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DruidOperatorTable implements SqlOperatorTable
{
  private static final Logger LOG = new Logger(DruidOperatorTable.class);

  private static final SqlOperatorTable STD = SqlStdOperatorTable.instance();

  private static final List<SqlAggregator> STANDARD_AGGREGATORS =
      ImmutableList.<SqlAggregator>builder()
          .add(new ApproxCountDistinctSqlAggregator())
          .add(new AvgSqlAggregator())
          .add(new CountSqlAggregator())
          .add(new MinSqlAggregator())
          .add(new MaxSqlAggregator())
          .add(new SumSqlAggregator())
          .add(new SumZeroSqlAggregator())
          .add(new RelayAggregator(new RelayAggFunction("ANY"), RelayType.FIRST))
          .add(new RelayAggregator(new RelayAggFunction("EARLIEST"), RelayType.TIME_MIN))
          .add(new RelayAggregator(new RelayAggFunction("LATEST"), RelayType.TIME_MAX))
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
          .add(new DirectOperatorConversion(SqlStdOperatorTable.JSON_VALUE, "json_value"))
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
          .add(new TimeCeilOperatorConversion())
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
          .add(new ArrayConstructorOperatorConversion())
          .add(new LeftOperatorConversion())
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LIKE, "LIKE"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ROW, "ARRAY"))
          .add(new LPadOperatorConversion())
          .add(new RegexpLikeOperatorConversion())
          .add(new RepeatOperatorConversion())
          .add(new RightOperatorConversion())
          .add(new RoundOperatorConversion())
          .add(new RPadOperatorConversion())
          .add(new SizeOperatorConversion())
          .add(new StringFormatOperatorConversion())
          .add(new ItemOperatorConversion())
          .add(new NominalBitSetToStringConversion())
          .add(new BitSetUnwrapConversion())
          .add(new BitSetCardinalityConversion())
          .add(new NominalTypeConversion())
          .add(new DedupOperatorConversion())
          .add(new DirectOperatorConversion(SqlStdOperatorTable.COALESCE, "COALESCE"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.INITCAP, "INITCAP"))
          .build();

  // Operators that have no conversion, but are handled in the convertlet table, so they still need to exist.
  private static final Map<OperatorKey, SqlOperator> CONVERTLET_OPERATORS =
      DruidConvertletTable.knownOperators()
                          .stream()
                          .collect(Collectors.toMap(OperatorKey::of, java.util.function.Function.identity()));

  private final Map<OperatorKey, SqlAggregator> aggregators;
  private final Map<OperatorKey, SqlOperatorConversion> operatorConversions;
  private final Map<OperatorKey, DimFilterConversion> dimFilterConversions;

  @Inject
  public DruidOperatorTable(
      final Set<SqlAggregator> userAggregators,
      final Set<AggregatorFactory.SQLBundle> userAggregatorFactories,
      final Set<SqlOperatorConversion> userOperatorConversions,
      final Set<DimFilterConversion> userDimFilterConversions
  )
  {
    this.aggregators = new HashMap<>();
    this.operatorConversions = new HashMap<>();
    this.dimFilterConversions = new HashMap<>();

    for (SqlAggregator aggregator : userAggregators) {
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction(), true);
      if (aggregators.put(operatorKey, aggregator) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (SqlAggregator aggregator : STANDARD_AGGREGATORS) {
      aggregators.putIfAbsent(OperatorKey.of(aggregator.calciteFunction()), aggregator);
    }

    for (AggregatorFactory.SQLBundle bundle : userAggregatorFactories) {
      final AggregatorFactory factory = (AggregatorFactory) bundle.aggregator;
      final ValueDesc type = Optional.fromNullable(factory.finalizedType()).or(ValueDesc.UNKNOWN);
      final SqlReturnTypeInference retType = ReturnTypes.explicit(Calcites.asRelDataType(type));
      final SqlAggFunction function = new DummyAggregatorFunction(bundle.opName, retType);
      final SqlAggregator aggregator = new DummySqlAggregator(function, bundle.aggregator, bundle.postAggregator);
      final OperatorKey operatorKey = OperatorKey.of(aggregator.calciteFunction(), true);
      if (aggregators.putIfAbsent(operatorKey, aggregator) == null) {
        LOG.info("> UDAF '%s' is registered with '%s'", bundle.opName, factory.getClass().getName());
      }
    }

    for (SqlOperatorConversion operatorConversion : userOperatorConversions) {
      final OperatorKey operatorKey = OperatorKey.of(operatorConversion.calciteOperator(), true);
      if (aggregators.containsKey(operatorKey) || operatorConversions.put(operatorKey, operatorConversion) != null) {
        throw new ISE("Cannot have two operators with key[%s]", operatorKey);
      }
    }

    for (DimFilterConversion conversion : userDimFilterConversions) {
      if (dimFilterConversions.putIfAbsent(OperatorKey.of(conversion.calciteOperator(), true), conversion) == null) {
        LOG.info("> SQL Filter '%s' is registered with %s", conversion.name(), conversion);
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
      if (factory instanceof Function.FixedTyped) {
        ValueDesc type = ((Function.FixedTyped) factory).returns();
        if (type == null || type.asClass() == Object.class) {
          continue;
        }
        retType = ReturnTypes.explicit(Calcites.asRelDataType(type));
      } else {
        retType = new SqlReturnTypeInference()
        {
          @Override
          public RelDataType inferReturnType(SqlOperatorBinding opBinding)
          {
            List<Expr> operands = Lists.newArrayList();
            Map<String, ValueDesc> binding = Maps.newHashMap();
            for (int i = 0; i < opBinding.getOperandCount(); i++) {
              final ValueDesc type = Calcites.asValueDesc(opBinding.getOperandType(i));
              if (opBinding.isOperandNull(i, false)) {
                operands.add(Evals.constant(null, type));
              } else if (opBinding.isOperandLiteral(i, false)) {
                operands.add(Evals.constant(opBinding.getOperandLiteralValue(i, type.asClass()), type));
              } else {
                String identifier = "p" + i;
                operands.add(Evals.identifierExpr(identifier, type));
                binding.put(identifier, type);
              }
            }
            TypeResolver resolver;
            if (factory instanceof WindowFunctions.Factory) {
              resolver = WindowContext.newInstance(Lists.newArrayList(binding.keySet()), binding);
            } else {
              resolver = Parser.withTypeMap(binding);
            }
            ValueDesc type = factory.create(operands, resolver).returns();
            if (type != null && type.asClass() != null) {
              return Calcites.asRelDataType(type);
            }
            return null;
          }
        };
      }

      String name = factory.name().toUpperCase();
      SqlOperator operator;
      if (factory instanceof WindowFunctions.Factory) {
        operator = new DummyAggregatorFunction(name, retType);
      } else {
        operator = new DummySqlFunction(name, retType);
      }

      OperatorKey operatorKey = OperatorKey.of(operator, !Parser.isBuiltIn(factory));
      if (!aggregators.containsKey(operatorKey)) {
        operatorConversions.putIfAbsent(operatorKey, new DirectOperatorConversion(operator, name));
      }
    }
  }

  public static String getFieldName(RexNode operand, PlannerContext plannerContext, RowSignature rowSignature)
  {
    DruidExpression expression = Expressions.toDruidExpression(plannerContext, rowSignature, operand);
    if (expression.isDirectColumnAccess()) {
      return expression.getDirectColumn();
    } else {
      return StringUtils.unquote(StringEscapeUtils.unescapeJava(expression.getExpression()));
    }
  }

  public boolean lookupAggregator(
      Aggregations aggregations,
      String outputName,
      AggregateCall aggregateCall,
      DimFilter predicate
  )
  {
    final SqlAggFunction aggregation = aggregateCall.getAggregation();
    final SqlAggregator sqlAggregator = aggregators.get(OperatorKey.of(aggregation));
    return sqlAggregator != null && sqlAggregator.register(aggregations, predicate, aggregateCall, outputName);
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

  public DimFilterConversion lookupDimFilterConversion(final SqlOperator operator)
  {
    final OperatorKey operatorKey = OperatorKey.of(operator);
    final DimFilterConversion operatorConversion = dimFilterConversions.get(operatorKey);
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
      final List<SqlOperator> operatorList,
      final SqlNameMatcher sqlNameMatcher
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
    final DimFilterConversion dimFilterConversion = dimFilterConversions.get(operatorKey);
    if (dimFilterConversion != null) {
      operatorList.add(dimFilterConversion.calciteOperator());
    }

    final SqlOperator convertletOperator = CONVERTLET_OPERATORS.get(operatorKey);
    if (operatorList.isEmpty() && convertletOperator != null) {
      operatorList.add(convertletOperator);
    }

    if (operatorList.isEmpty()) {
      STD.lookupOperatorOverloads(opName, category, syntax, operatorList, sqlNameMatcher);
    }
    if (operatorList.isEmpty()) {
      final OperatorKey windowKey = OperatorKey.of("$" + opName.getSimple(), syntax);
      final SqlOperatorConversion windowConversion = operatorConversions.get(windowKey);
      if (windowConversion != null) {
        operatorList.add(windowConversion.calciteOperator());
      }
    }
  }

  @Override
  public List<SqlOperator> getOperatorList()
  {
    Set<SqlOperator> operators = Sets.newHashSet(STD.getOperatorList());
    for (SqlAggregator aggregator : aggregators.values()) {
      operators.add(aggregator.calciteFunction());
    }
    for (SqlOperatorConversion operatorConversion : operatorConversions.values()) {
      operators.add(operatorConversion.calciteOperator());
    }
    for (DimFilterConversion dimFilterConversion : dimFilterConversions.values()) {
      operators.add(dimFilterConversion.calciteOperator());
    }
    operators.addAll(DruidConvertletTable.knownOperators());
    return Lists.newArrayList(operators);
  }

  public Map<OperatorKey, SqlAggregator> getAggregators()
  {
    return ImmutableMap.copyOf(aggregators);
  }

  public Map<OperatorKey, SqlOperatorConversion> getOperatorConversions()
  {
    return ImmutableMap.copyOf(operatorConversions);
  }

  public Map<OperatorKey, DimFilterConversion> getDimFilterConversions()
  {
    return ImmutableMap.copyOf(dimFilterConversions);
  }

  private static class DummySqlAggregator implements SqlAggregator
  {
    private final SqlAggFunction function;
    private final AggregatorFactory.SQLSupport aggregator;
    private final PostAggregator.SQLSupport postAggregator;

    private DummySqlAggregator(
        SqlAggFunction function,
        AggregatorFactory.SQLSupport aggregator,
        PostAggregator.SQLSupport postAggregator
    )
    {
      this.function = function;
      this.aggregator = aggregator;
      this.postAggregator = postAggregator;
    }

    @Override
    public SqlAggFunction calciteFunction()
    {
      return function;
    }

    @Override
    public boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName)
    {
      if (call.isDistinct()) {
        return false;
      }
      final List<DruidExpression> arguments = aggregations.argumentsToExpressions(call.getArgList());
      if (arguments == null) {
        return false;
      }
      List<String> fieldNames = arguments.stream()
                                         .map(expr -> aggregations.registerColumn(outputName, expr))
                                         .collect(Collectors.toList());

      if (postAggregator != null || aggregations.isFinalizing()) {
        final String aggregatorName = Calcites.makePrefixedName(outputName, "a");
        final AggregatorFactory rewritten = aggregator.rewrite(aggregatorName, fieldNames, aggregations.asTypeSolver());
        if (rewritten != null) {
          aggregations.register(rewritten, predicate);
          aggregations.register(postAggregator(outputName, rewritten));
          return true;
        }
      } else {
        final AggregatorFactory rewritten = aggregator.rewrite(outputName, fieldNames, aggregations.asTypeSolver());
        if (rewritten != null) {
          aggregations.register(rewritten, predicate);
          return true;
        }
      }
      return false;
    }

    private PostAggregator postAggregator(String name, AggregatorFactory rewritten)
    {
      return postAggregator != null
             ? postAggregator.rewrite(name, rewritten.getName())
             : AggregatorFactory.asFinalizer(name, rewritten);
    }
  }

  public static SqlFunction dummy(String name, SqlReturnTypeInference retType)
  {
    return new DummySqlFunction(name, retType);
  }

  private static class DummySqlFunction extends SqlFunction
  {
    public DummySqlFunction(String name, SqlReturnTypeInference retType)
    {
      super(
          name,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.cascade(retType, SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.SYSTEM
      );
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
          ReturnTypes.cascade(retType, SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.SYSTEM,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }

  private static class RelayAggFunction extends SqlAggFunction
  {
    RelayAggFunction(String name)
    {
      super(
          name,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          InferTypes.RETURN_TYPE,
          OperandTypes.ANY,
          SqlFunctionCategory.SYSTEM,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
