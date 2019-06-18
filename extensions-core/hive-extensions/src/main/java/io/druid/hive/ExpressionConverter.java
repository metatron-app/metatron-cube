/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.hive;

import com.google.common.base.Function;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Ranges;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.UDFToBoolean;
import org.apache.hadoop.hive.ql.udf.UDFToByte;
import org.apache.hadoop.hive.ql.udf.UDFToDouble;
import org.apache.hadoop.hive.ql.udf.UDFToFloat;
import org.apache.hadoop.hive.ql.udf.UDFToInteger;
import org.apache.hadoop.hive.ql.udf.UDFToLong;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.UDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.parquet.Strings;
import org.joda.time.Interval;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ExpressionConverter
{
  private static final Logger logger = new Logger(ExpressionConverter.class);

  static ExprNodeGenericFuncDesc deserializeExprDesc(Configuration configuration)
  {
    String filterExprSerialized = configuration.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    return filterExprSerialized == null ? null : SerializationUtilities.deserializeExpression(filterExprSerialized);
  }

  public static List<Interval> toInterval(List<Range> ranges)
  {
    List<Interval> intervals = Lists.transform(
        ranges, new Function<Range, Interval>()
        {
          @Override
          public Interval apply(Range range)
          {
            long start = range.hasLowerBound() ? toLong(range.lowerEndpoint()) : JodaUtils.MIN_INSTANT;
            long end = range.hasUpperBound() ? toLong(range.upperEndpoint()) : JodaUtils.MAX_INSTANT;
            if (range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN) {
              start++;
            }
            if (range.hasUpperBound() && range.upperBoundType() == BoundType.CLOSED) {
              end++;
            }
            return new Interval(start, end);
          }
        }
    );
    logger.info("Converted time ranges %s to interval %s", ranges, intervals);
    return intervals;
  }

  public static Map<String, TypeInfo> getColumnTypes(Configuration configuration, String timeColumnName)
  {
    String[] colNames = configuration.getStrings(serdeConstants.LIST_COLUMNS);
    List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(configuration.get(serdeConstants.LIST_COLUMN_TYPES));
    Set<Integer> projections = Sets.newHashSet(ColumnProjectionUtils.getReadColumnIDs(configuration));
    if (colNames == null || colTypes == null) {
      return ImmutableMap.of();
    }

    Map<String, TypeInfo> typeMap = Maps.newHashMap();
    for (int i = 0; i < colNames.length; i++) {
      if (!projections.isEmpty() && !projections.contains(i)) {
        continue;
      }
      String colName = colNames[i].trim();
      TypeInfo typeInfo = colTypes.get(i);
      if (colName.equals(timeColumnName) &&
          (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE ||
           ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.LONG &&
           ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()
           != PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP)) {
        logger.warn("time column should be defined as bigint or timestamp type");
      }
      typeMap.put(colName, typeInfo);
    }
    return typeMap;
  }

  private static ExprNodeDesc rewriteIfNeeded(ExprNodeGenericFuncDesc expr)
  {
    List<ExprNodeDesc> children = expr.getChildren();
    int result = -1;
    Pair<ExprNodeColumnDesc, PrimitiveTypeInfo> current = null;
    for (int i = 0; i < children.size(); i++) {
      ExprNodeDesc child = children.get(i);
      Pair<ExprNodeColumnDesc, PrimitiveTypeInfo> extracted = extractColumn(child);
      if (extracted == null || extracted.rhs == null) {
        if (!(child instanceof ExprNodeConstantDesc) ||
            !(child.getTypeInfo() instanceof PrimitiveTypeInfo)) {
          return expr;
        }
        continue;
      }
      if (current != null) {
        return expr;
      }
      current = extracted;
      result = i;
    }
    if (current == null) {
      return expr;
    }
    List<ExprNodeDesc> converted = Lists.newArrayList();
    for (int i = 0; i < children.size(); i++) {
      ExprNodeDesc child = children.get(i);
      if (i == result) {
        converted.add(current.lhs);
        continue;
      }
      ExprNodeConstantDesc constant = (ExprNodeConstantDesc) child;
      Object value = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector((PrimitiveTypeInfo) constant.getTypeInfo()),
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(current.rhs)
      ).convert(constant.getValue());
      converted.add(new ExprNodeConstantDesc(current.rhs, value));
    }
    return new ExprNodeGenericFuncDesc(expr.getTypeInfo(), expr.getGenericUDF(), expr.getFuncText(), converted);
  }

  private static Pair<ExprNodeColumnDesc, PrimitiveTypeInfo> extractColumn(ExprNodeDesc expr)
  {
    if (expr instanceof ExprNodeColumnDesc) {
      return Pair.of((ExprNodeColumnDesc) expr, null);
    }
    if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
      PrimitiveTypeInfo typeInfo = getCastType(func.getGenericUDF());
      if (typeInfo != null) {
        ExprNodeDesc child = Iterables.getOnlyElement(func.getChildren());
        if (child instanceof ExprNodeColumnDesc) {
          return Pair.of((ExprNodeColumnDesc) child, typeInfo);
        }
      }
    }
    return null;
  }

  private static PrimitiveTypeInfo getCastType(GenericUDF genericUDF)
  {
    Class udfClass = (genericUDF instanceof GenericUDFBridge) ?
                     ((GenericUDFBridge) genericUDF).getUdfClass() : genericUDF.getClass();
    if (udfClass == UDFToBoolean.class) {
      return TypeInfoFactory.booleanTypeInfo;
    } else if (udfClass == UDFToByte.class) {
      return TypeInfoFactory.byteTypeInfo;
    } else if (udfClass == UDFToDouble.class) {
      return TypeInfoFactory.doubleTypeInfo;
    } else if (udfClass == UDFToFloat.class) {
      return TypeInfoFactory.floatTypeInfo;
    } else if (udfClass == UDFToInteger.class) {
      return TypeInfoFactory.intTypeInfo;
    } else if (udfClass == UDFToLong.class) {
      return TypeInfoFactory.longTypeInfo;
    } else if (udfClass == UDFToShort.class) {
      return TypeInfoFactory.shortTypeInfo;
    } else if (udfClass == UDFToString.class) {
      return TypeInfoFactory.stringTypeInfo;
    } else if (udfClass == GenericUDFToVarchar.class) {
      return TypeInfoFactory.varcharTypeInfo;
    } else if (udfClass == GenericUDFToChar.class) {
      return TypeInfoFactory.charTypeInfo;
    } else if (udfClass == GenericUDFTimestamp.class) {
      return TypeInfoFactory.timestampTypeInfo;
    } else if (udfClass == GenericUDFToBinary.class) {
      return TypeInfoFactory.binaryTypeInfo;
    } else if (udfClass == GenericUDFToDate.class) {
      return TypeInfoFactory.dateTypeInfo;
    } else if (udfClass == GenericUDFToDecimal.class) {
      return TypeInfoFactory.decimalTypeInfo;
    }
    return null;
  }

  private static ExprNodeDesc revertCast(ExprNodeDesc expr)
  {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) expr;
      Class<?> op = funcDesc.getGenericUDF().getClass();

      if (op == GenericUDFOPOr.class ||
          op == GenericUDFOPAnd.class ||
          op == GenericUDFOPNot.class ||
          op == GenericUDFOPEqual.class ||
          op == GenericUDFOPNotEqual.class ||
          op == GenericUDFOPEqualNS.class ||
          op == GenericUDFOPGreaterThan.class ||
          op == GenericUDFOPEqualOrGreaterThan.class ||
          op == GenericUDFOPLessThan.class ||
          op == GenericUDFOPEqualOrLessThan.class ||
          op == GenericUDFIn.class ||
          op == GenericUDFBetween.class) {
        return rewriteIfNeeded(funcDesc);
      }
      List<ExprNodeDesc> children = Lists.newArrayList();
      for (ExprNodeDesc child : expr.getChildren()) {
        children.add(revertCast(child));
      }
      return new ExprNodeGenericFuncDesc(
          funcDesc.getTypeInfo(),
          funcDesc.getGenericUDF(),
          funcDesc.getFuncText(),
          children
      );
    }
    return expr;
  }

  static Map<String, List<Range>> getRanges(ExprNodeGenericFuncDesc filterExpr, Map<String, TypeInfo> types)
  {
    return getRanges(filterExpr, types, true);
  }

  static Map<String, List<Range>> getRanges(
      ExprNodeGenericFuncDesc filterExpr,
      Map<String, TypeInfo> types,
      boolean revertCast
  )
  {
    if (filterExpr == null) {
      return Maps.newHashMap();
    }
    logger.info("Start analyzing predicate %s", filterExpr.getExprString());
    if (revertCast) {
      ExprNodeGenericFuncDesc expression = (ExprNodeGenericFuncDesc) revertCast(filterExpr);
      if (filterExpr != expression) {
        logger.info("Rewritten predicate %s", expression.getExprString());
        filterExpr = expression;
      }
    }
    Configuration configuration = new Configuration();
    configuration.set("hive.io.filter.expr.serialized", SerializationUtilities.serializeExpression(filterExpr));
    SearchArgument searchArgument = ConvertAstToSearchArg.createFromConf(configuration);
    ExpressionTree root = searchArgument.getExpression();

    List<PredicateLeaf> leaves = Lists.newArrayList(searchArgument.getLeaves());

    Map<String, List<Range>> rangeMap = Maps.newHashMap();
    if (root.getOperator() == ExpressionTree.Operator.AND) {
      for (ExpressionTree child : root.getChildren()) {
        String extracted = extractSoleColumn(child, leaves);
        if (extracted == null) {
          continue;
        }
        TypeInfo type = types.get(extracted);
        if (type.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          continue;
        }
        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
        List<Range> ranges = extractRanges(ptype, child, leaves, false);
        if (ranges == null) {
          continue;
        }
        if (rangeMap.get(extracted) == null) {
          rangeMap.put(extracted, ranges);
          continue;
        }
        // (a or b) and (c or d) -> (a and c) or (b and c) or (a and d) or (b and d)
        List<Range> overlapped = Lists.newArrayList();
        for (Range current : rangeMap.get(extracted)) {
          for (Range interval : ranges) {
            if (current.isConnected(interval)) {
              overlapped.add(current.intersection(interval));
            }
          }
          rangeMap.put(extracted, overlapped);
        }
      }
    } else {
      String extracted = extractSoleColumn(root, leaves);
      if (extracted != null) {
        TypeInfo type = types.get(extracted);
        if (type.getCategory() == ObjectInspector.Category.PRIMITIVE) {
          PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
          List<Range> ranges = extractRanges(ptype, root, leaves, false);
          if (ranges != null) {
            rangeMap.put(extracted, ranges);
          }
        }
      }
    }

    Map<String, List<Range>> rangesMap = Maps.transformValues(rangeMap, Ranges.COMPACT);
    for (Map.Entry<String, List<Range>> entry : rangesMap.entrySet()) {
      logger.info(">> %s", entry);
    }
    return rangesMap;
  }

  private static String extractSoleColumn(ExpressionTree tree, List<PredicateLeaf> leaves)
  {
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      return leaves.get(tree.getLeaf()).getColumnName();
    }
    String current = null;
    List<ExpressionTree> children = tree.getChildren();
    if (children != null && !children.isEmpty()) {
      for (ExpressionTree child : children) {
        String resolved = extractSoleColumn(child, leaves);
        if (current != null && !current.equals(resolved)) {
          return null;
        }
        current = resolved;
      }
    }
    return current;
  }

  private static List<Range> extractRanges(
      PrimitiveTypeInfo type,
      ExpressionTree tree,
      List<PredicateLeaf> leaves,
      boolean withNot
  )
  {
    if (tree.getOperator() == ExpressionTree.Operator.NOT) {
      return extractRanges(type, tree.getChildren().get(0), leaves, !withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.LEAF) {
      return leafToRanges(type, leaves.get(tree.getLeaf()), withNot);
    }
    if (tree.getOperator() == ExpressionTree.Operator.OR) {
      List<Range> intervals = Lists.newArrayList();
      for (ExpressionTree child : tree.getChildren()) {
        List<Range> extracted = extractRanges(type, child, leaves, withNot);
        if (extracted != null) {
          intervals.addAll(extracted);
        }
      }
      return intervals;
    }
    return null;
  }

  private static List<Range> leafToRanges(PrimitiveTypeInfo type, PredicateLeaf hiveLeaf, boolean withNot)
  {
    PredicateLeaf.Operator operator = hiveLeaf.getOperator();
    switch (operator) {
      case LESS_THAN:
      case LESS_THAN_EQUALS:
      case EQUALS:  // in druid, all equals are null-safe equals
      case NULL_SAFE_EQUALS:
        Comparable value = literalToType(hiveLeaf.getLiteral(), type);
        if (value == null) {
          return null;
        }
        if (operator == PredicateLeaf.Operator.LESS_THAN) {
          return Arrays.<Range>asList(withNot ? Range.atLeast(value) : Range.lessThan(value));
        } else if (operator == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
          return Arrays.<Range>asList(withNot ? Range.greaterThan(value) : Range.atMost(value));
        } else {
          if (!withNot) {
            return Arrays.<Range>asList(Range.closed(value, value));
          }
          return Arrays.<Range>asList(Range.lessThan(value), Range.greaterThan(value));
        }
      case BETWEEN:
        Comparable value1 = literalToType(hiveLeaf.getLiteralList().get(0), type);
        Comparable value2 = literalToType(hiveLeaf.getLiteralList().get(1), type);
        if (value1 == null || value2 == null) {
          return null;
        }
        boolean inverted = value1.compareTo(value2) > 0;
        if (!withNot) {
          return Arrays.<Range>asList(inverted ? Range.closed(value2, value1) : Range.closed(value1, value2));
        }
        return Arrays.<Range>asList(
            Range.lessThan(inverted ? value2 : value1),
            Range.greaterThan(inverted ? value1 : value2)
        );
      case IN:
        List<Range> ranges = Lists.newArrayList();
        for (Object literal : hiveLeaf.getLiteralList()) {
          Comparable element = literalToType(literal, type);
          if (element == null) {
            return null;
          }
          if (withNot) {
            ranges.addAll(Arrays.<Range>asList(Range.lessThan(element), Range.greaterThan(element)));
          } else {
            ranges.add(Range.closed(element, element));
          }
        }
        return ranges;
    }
    return null;
  }

  // enforce list
  static Function<Object, Object> listConverter()
  {
    return new Function<Object, Object>()
    {
      @Override
      public Object apply(Object input)
      {
        if (input == null || input instanceof List || input.getClass().isArray()) {
          return input;
        }
        return Arrays.asList(input);
      }
    };
  }

  static Function<Object, Object> converter(PrimitiveTypeInfo type)
  {
    switch (type.getPrimitiveCategory()) {
      case BOOLEAN:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toBoolean(input);
          }
        };
      case BYTE:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toByte(input);
          }
        };
      case SHORT:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toShort(input);
          }
        };
      case INT:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toInt(input);
          }
        };
      case LONG:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toLong(input);
          }
        };
      case FLOAT:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toFloat(input);
          }
        };
      case DOUBLE:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toDouble(input);
          }
        };
      case DECIMAL:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toDecimal(input);
          }
        };
      case STRING:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return input == null ? null : String.valueOf(input);
          }
        };
      case VARCHAR:
        final int length = ((VarcharTypeInfo) type).getLength();
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return input == null ? null : new HiveVarchar(String.valueOf(input), length);
          }
        };
      case DATE:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toDate(input);
          }
        };
      case TIMESTAMP:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toTimestamp(input);
          }
        };
      case BINARY:
        return new Function<Object, Object>()
        {
          @Override
          public Object apply(Object input)
          {
            return toBinary(input);
          }
        };
      default:
        throw new UnsupportedOperationException("Not supported type " + type);
    }
  }

  private static Comparable literalToType(Object literal, PrimitiveTypeInfo type)
  {
    switch (type.getPrimitiveCategory()) {
      case BYTE:
        return toByte(literal);
      case SHORT:
        return toShort(literal);
      case INT:
        return toInt(literal);
      case LONG:
        return toLong(literal);
      case FLOAT:
        return toFloat(literal);
      case DOUBLE:
        return toDouble(literal);
      case DECIMAL:
        return toDecimal(literal);
      case STRING:
        return String.valueOf(literal);
      case DATE:
        return toDate(literal);
      case TIMESTAMP:
        return toTimestamp(literal);
    }
    return null;
  }

  private static Comparable toTimestamp(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Timestamp) {
      return (Timestamp) literal;
    }
    if (literal instanceof java.util.Date) {
      return new Timestamp(((java.util.Date) literal).getTime());
    }
    if (literal instanceof Number) {
      return new Timestamp(((Number) literal).longValue());
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return new Timestamp(Long.valueOf(string));
      }
      try {
        return Timestamp.valueOf(string);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Comparable toDate(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Date) {
      return (Date) literal;
    }
    if (literal instanceof Timestamp) {
      return new Date(((Timestamp) literal).getTime());
    }
    if (literal instanceof java.util.Date) {
      return new Date(((java.util.Date) literal).getTime());
    }
    if (literal instanceof Number) {
      return new Date(((Number) literal).longValue());
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return new Date(Long.valueOf(string));
      }
      try {
        return Date.valueOf(string);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Long toLong(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).longValue();
    }
    if (literal instanceof Date) {
      return ((Date) literal).getTime();
    }
    if (literal instanceof Timestamp) {
      return ((Timestamp) literal).getTime();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return Long.valueOf(string);
      }
      try {
        return Timestamp.valueOf(string).getTime();
      }
      catch (IllegalArgumentException e) {
        // best effort. ignore
      }
    }
    return null;
  }

  private static Boolean toBoolean(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).doubleValue() > 0;
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      return Boolean.valueOf(string);
    }
    return null;
  }

  private static Byte toByte(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).byteValue();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return Byte.valueOf(string);
      }
      try {
        return Double.valueOf(string).byteValue();
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Short toShort(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).shortValue();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return Short.valueOf(string);
      }
      try {
        return Double.valueOf(string).shortValue();
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Integer toInt(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).intValue();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      if (StringUtils.isNumeric(string)) {
        return Integer.valueOf(string);
      }
      try {
        return Double.valueOf(string).intValue();
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Float toFloat(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).floatValue();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      try {
        return Double.valueOf(string).floatValue();
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static Double toDouble(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof Number) {
      return ((Number) literal).doubleValue();
    }
    if (literal instanceof String) {
      String string = (String) literal;
      if (Strings.isNullOrEmpty(string)) {
        return null;
      }
      try {
        return Double.valueOf(string);
      }
      catch (NumberFormatException e) {
        // ignore
      }
    }
    return null;
  }

  private static HiveDecimal toDecimal(Object literal)
  {
    Long longVal = toLong(literal);
    return longVal == null ? null : HiveDecimal.create(longVal);
  }

  private static byte[] toBinary(Object literal)
  {
    if (literal == null) {
      return null;
    }
    if (literal instanceof byte[]) {
      return (byte[]) literal;
    }
    if (literal instanceof String) {
      return Base64.decodeBase64(io.druid.common.utils.StringUtils.toUtf8((String) literal));
    }
    return null;
  }
}
