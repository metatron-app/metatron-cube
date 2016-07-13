/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.parquet.Strings;
import org.joda.time.Interval;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  // should be string type
  public static DimFilter toFilter(String dimension, List<Range> ranges)
  {
    Iterable<Range> filtered = Iterables.filter(ranges, Ranges.VALID);
    List<String> equalValues = Lists.newArrayList();
    List<DimFilter> dimFilters = Lists.newArrayList();
    for (Range range : filtered) {
      String lower = range.hasLowerBound() ? (String) range.lowerEndpoint() : null;
      String upper = range.hasUpperBound() ? (String) range.upperEndpoint() : null;
      if (lower == null && upper == null) {
        return null;
      }
      if (Objects.equals(lower, upper)) {
        equalValues.add(lower);
        continue;
      }
      boolean lowerStrict = range.hasLowerBound() && range.lowerBoundType() == BoundType.OPEN;
      boolean upperStrict = range.hasUpperBound() && range.upperBoundType() == BoundType.OPEN;
      dimFilters.add(new BoundDimFilter(dimension, lower, upper, lowerStrict, upperStrict, false, null));
    }
    if (equalValues.size() > 1) {
      dimFilters.add(new InDimFilter(dimension, equalValues, null));
    } else if (equalValues.size() == 1) {
      dimFilters.add(new SelectorDimFilter(dimension, equalValues.get(0), null));
    }
    DimFilter dimFilter = new OrDimFilter(dimFilters).optimize();
    logger.info("Converted dimension '%s' ranges %s to filter %s", dimension, ranges, dimFilter);
    return dimFilter;
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

  static Map<String, List<Range>> getRanges(ExprNodeGenericFuncDesc filterExpr, Map<String, TypeInfo> types)
  {
    logger.info("Start analyzing predicate " + filterExpr.getExprString());
    SearchArgument searchArgument = ConvertAstToSearchArg.create(filterExpr);
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
      logger.info(">> " + entry);
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
      return (Date)literal;
    }
    if (literal instanceof java.util.Date) {
      return new Date(((java.util.Date)literal).getTime());
    }
    if (literal instanceof Timestamp) {
      return new Date(((Timestamp) literal).getTime());
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
}