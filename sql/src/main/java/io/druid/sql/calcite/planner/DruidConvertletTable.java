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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.UOE;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonValueReturning;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.Symbolizable;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidConvertletTable implements SqlRexConvertletTable
{
  // Apply a convertlet that doesn't do anything other than a "dumb" call translation.
  private static final SqlRexConvertlet BYPASS_CONVERTLET = StandardConvertletTable.INSTANCE::convertCall;

  // Apply the default convertlet found in StandardConvertletTable, which may do "smart" things.
  private static final SqlRexConvertlet STANDARD_CONVERTLET =
      (cx, call) -> StandardConvertletTable.INSTANCE.get(call).convertCall(cx, call);

  private static final List<SqlOperator> CURRENT_TIME_CONVERTLET_OPERATORS =
      ImmutableList.<SqlOperator>builder()
          .add(SqlStdOperatorTable.CURRENT_TIMESTAMP)
          .add(SqlStdOperatorTable.CURRENT_TIME)
          .add(SqlStdOperatorTable.CURRENT_DATE)
          .add(SqlStdOperatorTable.LOCALTIMESTAMP)
          .add(SqlStdOperatorTable.LOCALTIME)
          .build();

  // Operators we don't have standard conversions for, but which can be converted into ones that do by
  // Calcite's StandardConvertletTable.
  private static final List<SqlOperator> STANDARD_CONVERTLET_OPERATORS =
      ImmutableList.<SqlOperator>builder()
          .add(SqlStdOperatorTable.ROW)
          .add(SqlStdOperatorTable.EXISTS)
          .add(SqlStdOperatorTable.NOT_IN)
          .add(SqlStdOperatorTable.NOT_LIKE)
          .add(SqlStdOperatorTable.BETWEEN)
          .add(SqlStdOperatorTable.NOT_BETWEEN)
          .add(SqlStdOperatorTable.SYMMETRIC_BETWEEN)
          .add(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN)
          .add(SqlStdOperatorTable.TIMESTAMP_ADD)
          .add(SqlStdOperatorTable.UNION)
          .add(SqlStdOperatorTable.UNION_ALL)
          .add(SqlStdOperatorTable.NULLIF)
          .add(SqlStdOperatorTable.COALESCE)
          .add(SqlLibraryOperators.NVL)
          .build();

  private final Map<SqlOperator, SqlRexConvertlet> table;

  public DruidConvertletTable(final PlannerContext plannerContext)
  {
    this.table = createConvertletMap(plannerContext);
  }

  @Override
  public SqlRexConvertlet get(SqlCall call)
  {
    if (call.getKind() == SqlKind.EXTRACT && call.getOperandList().get(1).getKind() != SqlKind.LITERAL) {
      // Avoid using the standard convertlet for EXTRACT(TIMEUNIT FROM col), since we want to handle it directly
      // in ExtractOperationConversion.
      return BYPASS_CONVERTLET;
    } else {
      final SqlRexConvertlet convertlet = table.get(call.getOperator());
      return convertlet != null ? convertlet : StandardConvertletTable.INSTANCE.get(call);
    }
  }

  public static List<SqlOperator> knownOperators()
  {
    final ArrayList<SqlOperator> retVal = new ArrayList<>(
        CURRENT_TIME_CONVERTLET_OPERATORS.size() + STANDARD_CONVERTLET_OPERATORS.size()
    );

    retVal.addAll(CURRENT_TIME_CONVERTLET_OPERATORS);
    retVal.addAll(STANDARD_CONVERTLET_OPERATORS);

    return retVal;
  }

  private static Map<SqlOperator, SqlRexConvertlet> createConvertletMap(final PlannerContext plannerContext)
  {
    final SqlRexConvertlet currentTimestampAndFriends = new CurrentTimestampAndFriendsConvertlet(plannerContext);
    final Map<SqlOperator, SqlRexConvertlet> table = new HashMap<>();

    for (SqlOperator operator : CURRENT_TIME_CONVERTLET_OPERATORS) {
      table.put(operator, currentTimestampAndFriends);
    }

    for (SqlOperator operator : STANDARD_CONVERTLET_OPERATORS) {
      table.put(operator, STANDARD_CONVERTLET);
    }

    table.put(SqlStdOperatorTable.JSON_VALUE, new JsonValueConvertlet());

    return table;
  }

  private static class CurrentTimestampAndFriendsConvertlet implements SqlRexConvertlet
  {
    private final PlannerContext plannerContext;

    public CurrentTimestampAndFriendsConvertlet(final PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      final SqlOperator operator = call.getOperator();
      if (operator.equals(SqlStdOperatorTable.CURRENT_TIMESTAMP)
          || operator.equals(SqlStdOperatorTable.LOCALTIMESTAMP)) {

        return cx.getRexBuilder().makeTimestampLiteral(
            Calcites.jodaToCalciteTimestampString(plannerContext.getLocalNow(), DateTimeZone.UTC), 0
        );
      } else if (operator.equals(SqlStdOperatorTable.CURRENT_TIME) || operator.equals(SqlStdOperatorTable.LOCALTIME)) {
        return cx.getRexBuilder().makeTimeLiteral(
            Calcites.jodaToCalciteTimeString(plannerContext.getLocalNow(), DateTimeZone.UTC), 0
        );
      } else if (operator.equals(SqlStdOperatorTable.CURRENT_DATE)) {
        return cx.getRexBuilder().makeDateLiteral(
            Calcites.jodaToCalciteDateString(
                plannerContext.getLocalNow().hourOfDay().roundFloorCopy(),
                DateTimeZone.UTC
            )
        );
      } else {
        throw new ISE("Should not have got here, operator was: %s", operator);
      }
    }
  }

  /**
   * JSON_VALUE(jsonValue, path [ RETURNING type ] [ { ERROR | NULL | DEFAULT expr } ON EMPTY ] [ { ERROR | NULL | DEFAULT expr } ON ERROR ] )
   */
  private static class JsonValueConvertlet implements SqlRexConvertlet
  {
    @Override
    public RexNode convertCall(final SqlRexContext cx, final SqlCall call)
    {
      List<SqlNode> operands = call.getOperandList();
      List<RexNode> prepared = Lists.newArrayList();
      prepared.add(cx.convertExpression(call.operand(0)));
      prepared.add(cx.convertExpression(call.operand(1)));

      // SqlJsonValueReturning, SqlJsonValueEmptyOrErrorBehavior, SqlJsonEmptyOrError
      RexBuilder builder = cx.getRexBuilder();
      RelDataTypeFactory factory = cx.getTypeFactory();

      RelDataType retType = factory.createSqlType(SqlTypeName.VARCHAR, 2000);   // SqlJsonValueFunction.getDefaultType
      if (operands.size() > 2) {
        int ix0 = findSymbol(operands, SqlJsonValueReturning.class);
        int ix1 = findSymbol(operands, SqlJsonValueEmptyOrErrorBehavior.class);
        int ix2 = findSymbol(operands, SqlJsonEmptyOrError.class);
        if (ix0 > 0) {
          SqlDataTypeSpec type = call.operand(ix0 + 1);
          retType = type.deriveType(cx.getValidator());
        }
        prepared.add(builder.makeLiteral(Calcites.asValueDesc(retType).toString()));
        if (ix1 > 0 && ix2 > 0) {
          switch ((SqlJsonEmptyOrError) SqlLiteral.value(call.operand(ix2))) {
            case ERROR:
              throw new UOE("Not supports ON ERROR");
            case EMPTY:
              switch ((SqlJsonValueEmptyOrErrorBehavior) SqlLiteral.value(call.operand(ix1))) {
                case ERROR:
                  throw new UOE("Not supports ERROR behavior");
                case NULL:
                  prepared.add(builder.makeNullLiteral(retType));
                  break;
                case DEFAULT:
                  prepared.add(cx.convertExpression(call.operand(ix1 + 1)));
                  break;
              }
          }
        }
      }
      return builder.makeCall(factory.createTypeWithNullability(retType, true), call.getOperator(), prepared);
    }
  }

  private static int findSymbol(List<SqlNode> nodes, Class<? extends Symbolizable> clazz)
  {
    for (int i = 0; i < nodes.size(); i++) {
      SqlNode node = nodes.get(i);
      if (node instanceof SqlLiteral && clazz.isInstance(((SqlLiteral) node).getValue())) {
        return i;
      }
    }
    return -1;
  }
}
