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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.collections.IntList;
import io.druid.data.ValueDesc;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
      int ref = getInputRef(nodes.get(i));
      if (ref < 0) {
        return null;
      }
      inputRefs[i] = ref;
    }
    return inputRefs;
  }

  public static int getInputRef(RexNode node)
  {
    ImmutableBitSet bits = RelOptUtil.InputFinder.bits(node);
    return bits.cardinality() != 1 ? -1 : bits.nextSetBit(0);
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

  public static IntList collectInputRefs(List<RexNode> nodes)
  {
    final IntList indices = new IntList();
    for (RexNode node : nodes) {
      node.accept(new RexShuttle()
      {
        @Override
        public RexNode visitInputRef(RexInputRef ref)
        {
          final int index = ref.getIndex();
          if (indices.indexOf(index) < 0) {
            indices.add(index);
          }
          return ref;
        }
      });
    }
    return indices;
  }

  public static int[] revert(int[] indices)
  {
    final int[] mapping = new int[Ints.max(indices) + 1];
    Arrays.fill(mapping, -1);
    for (int i = 0; i < indices.length; i++) {
      mapping[indices[i]] = i;
    }
    return mapping;
  }

  public static List<RexNode> rewrite(RexBuilder builder, List<RexNode> nodes, int[] mapping)
  {
    final List<RexNode> rewrite = Lists.newArrayList();
    for (RexNode node : nodes) {
      rewrite.add(node.accept(new RexShuttle()
      {
        @Override
        public RexNode visitInputRef(RexInputRef ref)
        {
          return builder.makeInputRef(ref.getType(), mapping[ref.getIndex()]);
        }
      }));
    }
    return rewrite;
  }

  public static RexNode rewrite(RexBuilder builder, RexNode node, int[] mapping)
  {
    return node.accept(new RexShuttle()
    {
      @Override
      public RexNode visitInputRef(RexInputRef ref)
      {
        return builder.makeInputRef(ref.getType(), mapping[ref.getIndex()]);
      }
    });
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
    return findRel(sourceRel, DruidRel.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> T findRel(RelNode sourceRel, Class<T> clazz)
  {
    RelNode rel = sourceRel;
    if (sourceRel instanceof RelSubset) {
      rel = ((RelSubset) sourceRel).getBest();
      if (rel == null) {
        for (RelNode candidate : ((RelSubset) sourceRel).getRelList()) {
          if (clazz.isInstance(candidate)) {
            return (T)candidate;
          }
        }
      }
    } else if (sourceRel instanceof HepRelVertex) {
      rel = ((HepRelVertex) sourceRel).getCurrentRel();
    }
    return clazz.isInstance(rel) ? (T) rel : null;
  }

  // keep the same convention with calcite (see SqlValidatorUtil.addFields)
  public static List<String> uniqueNames(List<String> names1, List<String> names2)
  {
    List<String> nameList = Lists.newArrayList();
    Set<String> uniqueNames = Sets.newHashSet();
    for (String name : Iterables.concat(names1, names2)) {
      // Ensure that name is unique from all previous field names
      if (uniqueNames.contains(name)) {
        String nameBase = name;
        for (int i = 0; ; i++) {
          name = nameBase + i;
          if (!uniqueNames.contains(name)) {
            break;
          }
        }
      }
      nameList.add(name);
      uniqueNames.add(name);
    }
    return nameList;
  }

  public static RelDataType asRelDataType(ValueDesc valueDesc)
  {
    if (ValueDesc.isGeometry(valueDesc)) {
      return TYPE_FACTORY.createJavaType(valueDesc.asClass());
    }
    Class clazz = valueDesc.asClass();
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
