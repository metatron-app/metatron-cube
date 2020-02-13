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

package io.druid.sql.calcite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.util.Pair;

import java.util.List;

public class Utils
{
  public static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();
  public static final RowSignature EMPTY_ROW_SIGNATURE = RowSignature.builder().build();

  public static SqlIdentifier zero(String name)
  {
    return new SqlIdentifier(name, SqlParserPos.ZERO);
  }

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

  public static boolean isInputRef(RexNode op)
  {
    return op.isA(SqlKind.INPUT_REF);
  }

  public static int[] getInputRefs(List<RexNode> nodes)
  {
    final int[] inputRefs = new int[nodes.size()];
    for (int i = 0; i < inputRefs.length; i++) {
      if (!isInputRef(nodes.get(i))) {
        return null;
      }
      inputRefs[i] = ((RexSlot) nodes.get(i)).getIndex();
    }
    return inputRefs;
  }

  public static String opName(RexNode op)
  {
    return op instanceof RexCall ? ((RexCall) op).getOperator().getName() : null;
  }

  public static boolean isAllInputRef(List<RexNode> nodes)
  {
    for (RexNode node : nodes) {
      if (!isInputRef(node)) {
        return false;
      }
    }
    return true;
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

  public static DruidRel getDruidRel(RelNode sourceRel)
  {
    RelNode rel = sourceRel;
    if (sourceRel instanceof RelSubset) {
      rel = ((RelSubset) sourceRel).getBest();
      if (rel == null) {
        for (RelNode candidate : ((RelSubset) sourceRel).getRelList()) {
          if (candidate instanceof DruidRel) {
            rel = candidate;
            break;
          }
        }
      }
    }
    return rel instanceof DruidRel ? (DruidRel) rel : null;
  }

  public static RelDataType asRelDataType(Class clazz)
  {
    return clazz != null && clazz != Object.class ? TYPE_FACTORY.createType(clazz) : TYPE_FACTORY.createUnknownType();
  }

  public static List<String> getFieldNames(RelRoot root)
  {
    List<String> names = Lists.newArrayList();
    for (Pair<Integer, String> pair : root.fields) {
      names.add(pair.right);
    }
    return names;
  }

  public static int[] getFieldIndices(RelRoot root)
  {
    List<Integer> indices = Lists.newArrayList();
    for (Pair<Integer, String> pair : root.fields) {
      indices.add(pair.left);
    }
    return Ints.toArray(indices);
  }
}
