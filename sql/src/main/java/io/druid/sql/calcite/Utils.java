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

package io.druid.sql.calcite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;

import java.util.List;

public class Utils
{
  public static SqlNode createCondition(SqlNode left, SqlOperator op, SqlNode right)
  {
    List<Object> listCondition = Lists.newArrayList();
    listCondition.add(left);
    listCondition.add(new SqlParserUtil.ToTreeListItem(op, SqlParserPos.ZERO));
    listCondition.add(right);

    return SqlParserUtil.toTree(listCondition);
  }

  public static boolean isOr(RexNode op)
  {
    return op instanceof RexCall && op.getKind() == SqlKind.OR;
  }

  public static boolean isAnd(RexNode op)
  {
    return op instanceof RexCall && op.getKind() == SqlKind.AND;
  }

  public static RexNode and(RexBuilder builder, List<RexNode> operands)
  {
    Preconditions.checkArgument(!operands.isEmpty());
    return operands.size() == 1 ? operands.get(0) : builder.makeCall(SqlStdOperatorTable.AND, operands);
  }

  public static RexNode or(RexBuilder builder, List<RexNode> operands)
  {
    Preconditions.checkArgument(!operands.isEmpty());
    return operands.size() == 1 ? operands.get(0) : builder.makeCall(SqlStdOperatorTable.OR, operands);
  }
}
