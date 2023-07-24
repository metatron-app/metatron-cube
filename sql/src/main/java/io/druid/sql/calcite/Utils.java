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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.collections.IntList;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.query.Queries;
import io.druid.query.TableDataSource;
import io.druid.query.filter.LikeDimFilter.LikeMatcher;
import io.druid.query.ordering.StringComparators;
import io.druid.sql.calcite.expression.SimpleExtraction;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidTypeSystem;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.QueryMaker;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES;

public class Utils
{
  public static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(DruidTypeSystem.INSTANCE);
  public static final RowSignature EMPTY_ROW_SIGNATURE = RowSignature.builder().build();

  public static final Set<SqlKind> COMPARISON = EnumSet.of(
      SqlKind.EQUALS,
      SqlKind.GREATER_THAN_OR_EQUAL,
      SqlKind.LESS_THAN_OR_EQUAL,
      SqlKind.GREATER_THAN,
      SqlKind.LESS_THAN,
      SqlKind.NOT_EQUALS
  );

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

  public static SqlNode frame(SqlNode frame, SqlNode increment, SqlNode offset)
  {
    return frame(Objects.toString(SqlLiteral.value(frame), ""), increment, offset, frame.getParserPosition());
  }

  public static SqlNode frame(String frame, SqlNode increment, SqlNode offset)
  {
    return frame(frame, increment, offset, increment.getParserPosition());
  }

  private static SqlNode frame(String frame, SqlNode increment, SqlNode offset, SqlParserPos pos)
  {
    String s2 = Objects.toString(increment == null ? null : SqlLiteral.value(increment), "1");
    String s3 = Objects.toString(offset == null ? null : SqlLiteral.value(offset), "");
    return SqlLiteral.createCharString(frame + ":" + s2 + ":" + s3, pos);
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

  public static boolean isA(RexNode op, SqlTypeName typeName)
  {
    return op.getType().getSqlTypeName() == typeName;
  }

  public static int[] getInputRefs(List<RexNode> nodes)
  {
    final int[] inputRefs = new int[nodes.size()];
    for (int i = 0; i < inputRefs.length; i++) {
      int ref = soleInputRef(nodes.get(i));
      if (ref < 0) {
        return null;
      }
      inputRefs[i] = ref;
    }
    return inputRefs;
  }

  public static int soleInputRef(RexNode node)
  {
    ImmutableBitSet bits = RelOptUtil.InputFinder.bits(node);
    return bits.cardinality() != 1 ? -1 : bits.nextSetBit(0);
  }

  public static boolean isSoleRef(RexNode node)
  {
    return soleInputRef(node) >= 0;
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

  public static IntList extractInputRef(List<RexNode> nodes)
  {
    final IntList indices = new IntList();
    for (RexNode node : nodes) {
      if (node.isA(SqlKind.INPUT_REF)) {
        indices.add(((RexInputRef) node).getIndex());
      } else if (node.isA(SqlKind.FIELD_ACCESS)) {
        indices.add(((RexFieldAccess) node).getField().getIndex());
      } else {
        return null;
      }
    }
    return indices;
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
    if (indices.length == 0) {
      return indices;
    }
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

  public static RexNode extractCommon(RexNode condition, RexBuilder builder)
  {
    return Utils.isOr(condition) ? RexUtil.pullFactors(builder, condition) : condition;
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
        if (sourceRel.getConvention() != Convention.NONE) {
          sourceRel = sourceRel.copy(sourceRel.getTraitSet().replace(Convention.NONE), ImmutableList.of());
        }
        for (RelNode candidate : ((RelSubset) sourceRel).getRelList()) {
          if (clazz.isInstance(candidate)) {
            return (T) candidate;
          }
        }
      }
    } else if (sourceRel instanceof HepRelVertex) {
      rel = ((HepRelVertex) sourceRel).getCurrentRel();
    }
    return clazz.isInstance(rel) ? (T) rel : null;
  }

  public static Pair<DruidRel, RelOptCost> getMinimumCost(
      RelNode inputRel,
      RelOptPlanner planner,
      RelMetadataQuery mq,
      Set<RelNode> visited
  )
  {
    RelNode rel = inputRel;
    if (inputRel instanceof RelSubset) {
      rel = ((RelSubset) inputRel).getBest();
      if (rel == null) {
        if (inputRel.getConvention() != Convention.NONE) {
          inputRel = inputRel.copy(inputRel.getTraitSet().replace(Convention.NONE), ImmutableList.of());
        }
        DruidRel source = null;
        RelOptCost minimum = null;
        for (RelNode candidate : ((RelSubset) inputRel).getRelList()) {
          if (candidate instanceof DruidRel) {
            RelOptCost current = ((DruidRel) candidate).computeSelfCost(planner, mq, visited);
            if (minimum == null || current.isLe(minimum)) {
              source = (DruidRel) candidate;
              minimum = current;
            }
          }
        }
        return Pair.of(source, minimum);
      }
    } else if (inputRel instanceof HepRelVertex) {
      rel = ((HepRelVertex) inputRel).getCurrentRel();
    }
    if (rel instanceof DruidRel) {
      final DruidRel source = (DruidRel) rel;
      return Pair.of(source, source.computeSelfCost(planner, mq, visited));
    } else {
      return Pair.of(null, planner.getCostFactory().makeInfiniteCost());
    }
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

  public static double joinCost(double rc1, double rc2)
  {
    double cost = Math.sqrt(Math.pow(Math.max(rc1, rc2), 2) + Math.pow(Math.min(rc1, rc2), 2));
    if (rc1 > rc2) {
      cost *= 0.999; // for deterministic plan
    }
    return cost;
  }

  public static long[] estimateSelectivity(String table, Filtration filtration, QueryMaker context)
  {
    return Queries.filterSelectivity(
        TableDataSource.of(table),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        context.getPlannerContext().getQueryContext(),
        context.getSegmentWalker()
    );
  }

  public static double selectivity(RexNode condition)
  {
    if (condition == null || condition.isAlwaysTrue()) {
      return 1.0D;
    }
    final double estimate;
    switch (condition.getKind()) {
      case AND:
        return ((RexCall) condition).getOperands().stream()
                                    .mapToDouble(op -> selectivity(op)).min().orElse(1);
      case OR:
        return Math.max(1, ((RexCall) condition).getOperands().stream()
                                                .mapToDouble(op -> selectivity(op)).sum());
      case NOT:
        return 1 - selectivity(((RexCall) condition).getOperands().get(0));
      case BETWEEN:
        return 0.3;
      case LIKE:
        return 0.6;
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return 0.5;
      case EQUALS:
        return 0.05;
      case NOT_EQUALS:
        return 0.95;
      case IS_NULL:
        return 0.02;
      case IS_NOT_NULL:
        return 0.98;
      default:
        return 0.8;
    }
  }

  private static final double REF_PER_COLUMN = 0.001;
  private static final double LITERAL_PER_COLUMN = 0.00001;

  private static final double CUSTOM_FN_CALL = 0.02;
  private static final double LIKE_PER_COLUMN = 0.05;
  private static final double EXPR_PER_COLUMN = 0.005;

  public static double rexEvalCost(List<RexNode> rexNodes)
  {
    return rexNodes.stream().mapToDouble(Utils::rexEvalCost).sum();
  }

  public static double rexEvalCost(RexNode rexNode)
  {
    switch (rexNode.getKind()) {
      case INPUT_REF:
        return REF_PER_COLUMN;
      case LITERAL:
        return LITERAL_PER_COLUMN;
    }
    double cost = 0;
    if (rexNode instanceof RexCall) {
      cost = rexEvalCost(((RexCall) rexNode).getOperands());
      switch (rexNode.getKind()) {
        case OTHER_FUNCTION:
          return CUSTOM_FN_CALL + cost;   // todo
        case LIKE:
          return LIKE_PER_COLUMN + cost;
      }
    }
    return EXPR_PER_COLUMN + cost;
  }

  private static final double TIMESERIES = 0.01;
  private static final double GROUP_BY_FACTOR = 1.4;

  public static double aggregationRow(int cardinality)
  {
    return cardinality == 0 ? TIMESERIES : Math.min(1, Math.pow(GROUP_BY_FACTOR, cardinality) - 1);
  }

  public static double aggregationCost(int dimensionality, List<AggregateCall> aggregations)
  {
    return Math.pow(1.2, dimensionality + Utils.aggregationCost(aggregations));
  }

  public static double aggregationCost(List<AggregateCall> aggregations)
  {
    return aggregations.stream().mapToDouble(Utils::aggregationCost).sum();
  }

  public static double aggregationCost(AggregateCall aggregation)
  {
    if (aggregation.isDistinct()) {
      return 0.5;
    }
    SqlAggFunction function = aggregation.getAggregation();
    switch (function.getName().toLowerCase()) {
      case "any":
        return 0.01;
      case "count":
        return aggregation.getArgList().isEmpty() ? 0.01 : 0.03;
      case "min":
      case "max":
      case "sum":
      case "avg":
        return 0.08;
      case "firstof":
      case "lastof":
      case "earliest":
      case "latest":
      case "minof":
      case "maxof":
        return 0.12;
      case "approx_count_distinct":
        return 0.5;
      default:
        return 0.1;   // stats
    }
  }

  public static List<RexNode> decomposeOnAnd(RexNode rexNode)
  {
    return rexNode.getKind() == SqlKind.AND ? ((RexCall) rexNode).getOperands() : Arrays.asList(rexNode);
  }

  public static Object extractLiteral(RexLiteral literal, PlannerContext context)
  {
    if (SqlTypeName.NUMERIC_TYPES.contains(literal.getTypeName())) {
      return RexLiteral.value(literal);
    } else if (SqlTypeName.CHAR_TYPES.contains(literal.getTypeName())) {
      return RexLiteral.stringValue(literal);
    } else if (SqlTypeName.TIMESTAMP == literal.getTypeName() || SqlTypeName.DATE == literal.getTypeName()) {
      return Calcites.calciteDateTimeLiteralToJoda(literal, context.getTimeZone()).getMillis();
    } else {
      // Don't know how to filter on this kind of literal.
      return null;
    }
  }

  public static int extractSimpleCastedColumn(RexNode rex)
  {
    if (rex.getKind() == SqlKind.CAST) {
      final ImmutableList<RexNode> operands = ((RexCall) rex).operands;
      if (operands.size() == 1 && operands.get(0).getKind() == SqlKind.INPUT_REF) {
        return ((RexInputRef) operands.get(0)).getIndex();
      }
    }
    return -1;
  }

  public static Predicate extractFilter(int ref, List<RexNode> filters)
  {
    final Iterator<RexNode> iterator = filters.iterator();
    while (iterator.hasNext()) {
      RexNode node = iterator.next();
      if (node.getKind() == SqlKind.EQUALS) {
        RexNode op1 = ((RexCall) node).getOperands().get(0);
        RexNode op2 = ((RexCall) node).getOperands().get(1);
        if (op1.getKind() == SqlKind.INPUT_REF && op2.getKind() == SqlKind.LITERAL) {
          if (ref == ((RexInputRef) op1).getIndex()) {
            iterator.remove();
            Object coereced = QueryMaker.coerece(RexLiteral.value(op2), op2.getType());
            return v -> Objects.equals(v, coereced);
          }
        } else if (op2.getKind() == SqlKind.INPUT_REF && op1.getKind() == SqlKind.LITERAL) {
          if (ref == ((RexInputRef) op2).getIndex()) {
            iterator.remove();
            Object coereced = QueryMaker.coerece(RexLiteral.value(op1), op1.getType());
            return v -> Objects.equals(v, coereced);
          }
        }
      } else if (node.getKind() == SqlKind.LIKE) {
        RexNode op1 = ((RexCall) node).getOperands().get(0);
        RexNode op2 = ((RexCall) node).getOperands().get(1);
        if (op1.getKind() == SqlKind.INPUT_REF && op2.getKind() == SqlKind.LITERAL) {
          if (ref == ((RexInputRef) op1).getIndex()) {
            iterator.remove();
            Object coereced = QueryMaker.coerece(RexLiteral.value(op2), op2.getType());
            return LikeMatcher.from(String.valueOf(coereced), null).asPredicate();
          }
        }
      } else if (node.getKind() == SqlKind.OR) {
        return extractIn(ref, (RexCall) node);
      }
    }
    return null;
  }

  private static Predicate extractIn(int ref, RexCall node)
  {
    for (RexNode rex : node.getOperands()) {
      if (rex.getKind() != SqlKind.EQUALS) {
        return null;
      }
    }
    Set<Object> values = Sets.newHashSet();
    for (RexNode rex : node.getOperands()) {
      RexNode op1 = ((RexCall) rex).getOperands().get(0);
      RexNode op2 = ((RexCall) rex).getOperands().get(1);
      if (op1.getKind() == SqlKind.INPUT_REF && op2.getKind() == SqlKind.LITERAL) {
        if (ref != ((RexInputRef) op1).getIndex()) {
          return null;
        }
        values.add(QueryMaker.coerece(RexLiteral.value(op2), op2.getType()));
      } else if (op2.getKind() == SqlKind.INPUT_REF && op1.getKind() == SqlKind.LITERAL) {
        if (ref != ((RexInputRef) op2).getIndex()) {
          return null;
        }
        values.add(QueryMaker.coerece(RexLiteral.value(op1), op1.getType()));
      }
    }
    return v -> values.contains(v);
  }

  @SuppressWarnings("unchecked")
  public static <T extends RelNode> T apply(T relNode, RexShuttle shuttle)
  {
    return relNode == null || shuttle == null ? relNode : (T) relNode.accept(shuttle);
  }

  public static String print(RelNode rel)
  {
    StringWriter sb = new StringWriter();
    rel.explain(new RelWriterImpl(new PrintWriter(sb), ALL_ATTRIBUTES, true));
    return sb.toString();
  }

  public static String alias(RelNode rel)
  {
    if (rel instanceof HepRelVertex) {
      rel = ((HepRelVertex) rel).getCurrentRel();
    }
    String tableName = tableName(rel.getTable());
    if (tableName != null) {
      return tableName;
    }
    if (rel instanceof BiRel) {
      return String.format("[%s + %s]", alias(((BiRel) rel).getLeft()), alias(((BiRel) rel).getRight()));
    }
    return alias(rel.getInput(0));
  }

  public static String tableName(RelOptTable table)
  {
    return table == null ? null : GuavaUtils.lastOf(table.getQualifiedName());
  }

  public static String qualifiedTableName(RelOptTable table)
  {
    return table == null ? null : StringUtils.join(table.getQualifiedName(), '.');
  }

  public static List<String> columnNames(RelNode rel, ImmutableBitSet bits)
  {
    List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    return Arrays.stream(bits.toArray()).mapToObj(x -> fields.get(x).getName()).collect(Collectors.toList());
  }

  public static String comparatorFor(TypeResolver resolver, SimpleExtraction extraction)
  {
    if (extraction.getExtractionFn() == null && resolver.isNumeric(extraction.getColumn())) {
      return StringComparators.NUMERIC_NAME;
    }
    return null;
  }
}
