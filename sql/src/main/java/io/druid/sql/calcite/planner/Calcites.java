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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Longs;
import io.druid.common.DateTimes;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.ordering.StringComparators;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.InformationSchema;
import io.druid.sql.calcite.schema.SystemSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.DruidType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import static io.druid.sql.calcite.Utils.TYPE_FACTORY;

/**
 * Utility functions for Calcite.
 */
public class Calcites
{
  private static final DateTimes.UtcFormatter CALCITE_DATE_PARSER = DateTimes.wrapFormatter(ISODateTimeFormat.dateParser());
  private static final DateTimes.UtcFormatter CALCITE_TIMESTAMP_PARSER = DateTimes.wrapFormatter(
      new DateTimeFormatterBuilder()
          .append(ISODateTimeFormat.dateParser())
          .appendLiteral(' ')
          .append(ISODateTimeFormat.timeParser())
          .toFormatter()
  );

  private static final DateTimeFormatter CALCITE_TIME_PRINTER = DateTimeFormat.forPattern("HH:mm:ss.S");
  private static final DateTimeFormatter CALCITE_DATE_PRINTER = DateTimeFormat.forPattern("yyyy-MM-dd");
  private static final DateTimeFormatter CALCITE_TIMESTAMP_PRINTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");

  private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

  private Calcites()
  {
    // No instantiation.
  }

  public static void setSystemProperties()
  {
    // These properties control the charsets used for SQL literals. I don't see a way to change this except through
    // system properties, so we'll have to set those...

    final String charset = ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
    System.setProperty("saffron.default.charset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.nationalcharset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.collation.name", StringUtils.format("%s$en_US", charset));
  }

  public static Charset defaultCharset()
  {
    return DEFAULT_CHARSET;
  }

  public static SchemaPlus createRootSchema(
      final DruidSchema druidSchema,
      final QuerySegmentWalker segmentWalker,
      final SystemSchema systemSchema
  )
  {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    rootSchema.add(DruidSchema.NAME, druidSchema);
    rootSchema.add(InformationSchema.NAME, new InformationSchema(rootSchema, segmentWalker));
    rootSchema.add(SystemSchema.NAME, systemSchema);
    return rootSchema;
  }

  public static String escapeStringLiteral(final String s)
  {
    Preconditions.checkNotNull(s);
    boolean isPlainAscii = true;
    final StringBuilder builder = new StringBuilder("'");
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || c == ' ') {
        builder.append(c);
        if (c > 127) {
          isPlainAscii = false;
        }
      } else {
        builder.append("\\").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
        isPlainAscii = false;
      }
    }
    builder.append("'");
    return isPlainAscii ? builder.toString() : "U&" + builder;

  }

  public static boolean isFloatCastedToDouble(RexNode rexNode)
  {
    return rexNode.isA(SqlKind.CAST) &&
           Calcites.getTypeName(rexNode.getType()) == SqlTypeName.DOUBLE &&
           Calcites.getTypeName(((RexCall) rexNode).getOperands().get(0).getType()) == SqlTypeName.FLOAT;
  }

  public static boolean isLiteralDecimalCastedToDouble(RexNode rexNode)
  {
    return rexNode.isA(SqlKind.LITERAL) &&
           getTypeName(rexNode.getType()) == SqlTypeName.DOUBLE &&
           ((RexLiteral) rexNode).getTypeName() == SqlTypeName.DECIMAL;
  }

  public static SqlTypeName getTypeName(RelDataType dataType)
  {
    if (dataType instanceof DruidType) {
      ValueDesc type = ((DruidType) dataType).getDruidType();
      if (type.isDimension() || type.isMultiValued()) {
        return SqlTypeName.VARCHAR;
      }
    }
    return dataType.getSqlTypeName();
  }

  public static ValueDesc toCoercedType(RelDataType dataType)
  {
    if (dataType instanceof DruidType) {
      ValueDesc druidType = ((DruidType) dataType).getDruidType();
      if (druidType.isDimension() || druidType.isMultiValued()) {
        return ValueDesc.STRING;
      }
      return druidType;
    }
    final SqlTypeName sqlTypeName = dataType.getSqlTypeName();
    if (SqlTypeName.TIMESTAMP == sqlTypeName || SqlTypeName.DATE == sqlTypeName) {
      return ValueDesc.STRING;  // I cannot understand this.. see SqlResource.execute
    }
    return asValueDesc(dataType);
  }

  public static ValueDesc asValueDesc(RexNode rexNode)
  {
    return asValueDesc(rexNode.getType());
  }

  public static ValueDesc asValueDesc(RelDataType dataType)
  {
    if (dataType instanceof DruidType) {
      return ((DruidType) dataType).getDruidType();
    }
    final SqlTypeName sqlTypeName = dataType.getSqlTypeName();
    if (SqlTypeName.BOOLEAN == sqlTypeName) {
      return ValueDesc.BOOLEAN;
    } else if (SqlTypeName.FLOAT == sqlTypeName || SqlTypeName.REAL == sqlTypeName) {
      return ValueDesc.FLOAT;
    } else if (SqlTypeName.DECIMAL == sqlTypeName) {
      return ValueDesc.DECIMAL;
    } else if (SqlTypeName.DOUBLE == sqlTypeName) {
      return ValueDesc.DOUBLE;
    } else if (SqlTypeName.TIMESTAMP == sqlTypeName
               || SqlTypeName.DATE == sqlTypeName
               || SqlTypeName.INT_TYPES.contains(sqlTypeName)) {
      return ValueDesc.LONG;
    } else if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      return ValueDesc.STRING;
    } else if (SqlTypeName.GEOMETRY == sqlTypeName) {
      return ValueDesc.GEOMETRY;    // it's not working (todo)
    } else if (SqlTypeName.ARRAY == sqlTypeName) {
      return ValueDesc.ofArray(asValueDesc(dataType.getComponentType()));
    } else if (SqlTypeName.MAP == sqlTypeName) {
      return ValueDesc.ofMap(
          asValueDesc(dataType.getKeyType()),
          asValueDesc(dataType.getValueType())
      );
    } else if (dataType.isStruct()) {
      List<RelDataTypeField> fields = dataType.getFieldList();
      String[] names = new String[fields.size()];
      ValueDesc[] types = new ValueDesc[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        RelDataTypeField field = fields.get(i);
        names[i] = field.getName();
        types[i] = asValueDesc(field.getType());
      }
      return ValueDesc.ofStruct(names, types);
    }
    // hack
    Type type = TYPE_FACTORY.getJavaClass(dataType);
    if (type != null && type.getTypeName().startsWith("org.locationtech.jts.geom.")) {
      return ValueDesc.GEOMETRY;
    }
    return ValueDesc.of(sqlTypeName.getName());
  }

  public static String getStringComparatorForDataType(RelDataType dataType)
  {
    final ValueDesc valueType = asValueDesc(dataType);
    return getStringComparatorForValueType(valueType);
  }

  public static String getStringComparatorForValueType(ValueDesc valueDesc)
  {
    if (valueDesc.unwrapDimension().isNumeric()) {
      return StringComparators.NUMERIC_NAME;
    } else {
      return null;    // regard as natural ordering
    }
  }

  public static RelDataType asRelDataType(RelDataTypeFactory typeFactory, Class javaType)
  {
    return typeFactory.createTypeWithNullability(typeFactory.createJavaType(javaType), true);
  }

  public static RelDataType asRelDataType(final String columnName, final ValueDesc columnType)
  {
    if (Row.TIME_COLUMN_NAME.equals(columnName)) {
      return Calcites.asRelDataType(SqlTypeName.TIMESTAMP);
    } else {
      return Calcites.asRelDataType(columnType);
    }
  }

  public static RelDataType asRelDataType(final SqlTypeName typeName)
  {
    return asRelDataType(TYPE_FACTORY, typeName, false);
  }

  /**
   * Like RelDataTypeFactory.createSqlType, but creates types that align best with how Druid represents them.
   */
  public static RelDataType asRelDataType(final RelDataTypeFactory typeFactory, final SqlTypeName typeName)
  {
    return asRelDataType(typeFactory, typeName, false);
  }

  /**
   * Like RelDataTypeFactory.createSqlTypeWithNullability, but creates types that align best with how Druid
   * represents them.
   */
  public static RelDataType asRelDataType(
      final RelDataTypeFactory typeFactory,
      final SqlTypeName typeName,
      final boolean nullable
  )
  {
    final RelDataType dataType;

    switch (typeName) {
      case TIMESTAMP:
        // Our timestamps are down to the millisecond (precision = 3).
        dataType = typeFactory.createSqlType(typeName, 3);
        break;
      case CHAR:
      case VARCHAR:
        dataType = typeFactory.createTypeWithCharsetAndCollation(
            typeFactory.createSqlType(typeName),
            Calcites.defaultCharset(),
            SqlCollation.IMPLICIT
        );
        break;
      case ARRAY:
        dataType = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.OTHER), -1);
        break;
      default:
        dataType = typeFactory.createSqlType(typeName);
    }

    return typeFactory.createTypeWithNullability(dataType, nullable);
  }

  public static Long coerceToTimestamp(final Object value, final DateTimeZone timeZone)
  {
    if (value == null) {
      return null;
    }
    DateTime dateTime = null;
    if (value instanceof DateTime) {
      dateTime = (DateTime) value;
    } else if (value instanceof Number) {
      final long timestamp = ((Number) value).longValue();
      if (timeZone == DateTimeZone.UTC) {
        return timestamp;
      }
      dateTime = DateTimes.utc(timestamp);
    } else if (value instanceof String) {
      final Long timestamp = Longs.tryParse((String) value);
      if (timestamp != null) {
        if (timeZone == DateTimeZone.UTC) {
          return timestamp;
        }
        dateTime = DateTimes.utc(timestamp);
      }
    }
    if (dateTime == null) {
      try {
        dateTime = new DateTime(value);
      }
      catch (Exception e) {
        return null;
      }
    }
    return jodaToCalciteTimestamp(dateTime, timeZone);
  }

  /**
   * Calcite expects "TIMESTAMP" types to be an instant that has the expected local time fields if printed as UTC.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style millis
   */
  public static long jodaToCalciteTimestamp(final DateTime dateTime, final DateTimeZone timeZone)
  {
    return dateTime.withZone(timeZone).withZoneRetainFields(DateTimeZone.UTC).getMillis();
  }

  /**
   * Calcite expects "DATE" types to be number of days from the epoch to the UTC date matching the local time fields.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style date
   */
  public static int jodaToCalciteDate(final DateTime dateTime, final DateTimeZone timeZone)
  {
    final DateTime date = dateTime.withZone(timeZone).dayOfMonth().roundFloorCopy();
    return Days.daysBetween(DateTimes.EPOCH, date.withZoneRetainFields(DateTimeZone.UTC)).getDays();
  }

  /**
   * Calcite expects TIMESTAMP literals to be represented by TimestampStrings in the local time zone.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static TimestampString jodaToCalciteTimestampString(final DateTime dateTime, final DateTimeZone timeZone)
  {
    // The replaceAll is because Calcite doesn't like trailing zeroes in its fractional seconds part.
    return new TimestampString(CALCITE_TIMESTAMP_PRINTER.print(dateTime.withZone(timeZone)).replaceAll("\\.?0+$", ""));
  }

  /**
   * Calcite expects TIME literals to be represented by TimeStrings in the local time zone.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static TimeString jodaToCalciteTimeString(final DateTime dateTime, final DateTimeZone timeZone)
  {
    // The replaceAll is because Calcite doesn't like trailing zeroes in its fractional seconds part.
    return new TimeString(CALCITE_TIME_PRINTER.print(dateTime.withZone(timeZone)).replaceAll("\\.?0+$", ""));
  }

  /**
   * Calcite expects DATE literals to be represented by DateStrings in the local time zone.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static DateString jodaToCalciteDateString(final DateTime dateTime, final DateTimeZone timeZone)
  {
    return new DateString(CALCITE_DATE_PRINTER.print(dateTime.withZone(timeZone)));
  }

  /**
   * Translates "literal" (a TIMESTAMP or DATE literal) to milliseconds since the epoch using the provided
   * session time zone.
   *
   * @param literal  TIMESTAMP or DATE literal
   * @param timeZone session time zone
   *
   * @return milliseconds time
   */
  public static DateTime calciteDateTimeLiteralToJoda(final RexNode literal, final DateTimeZone timeZone)
  {
    if (literal.getKind() != SqlKind.LITERAL) {
      throw new IAE("Expected literal but got [%s]", literal.getKind());
    }
    final SqlTypeName typeName = Calcites.getTypeName(literal.getType());
    if (typeName == SqlTypeName.TIMESTAMP) {
      final TimestampString timestampString = (TimestampString) RexLiteral.value(literal);
      return CALCITE_TIMESTAMP_PARSER.parse(timestampString.toString()).withZoneRetainFields(timeZone);
    } else if (typeName == SqlTypeName.DATE) {
      final DateString dateString = (DateString) RexLiteral.value(literal);
      return CALCITE_DATE_PARSER.parse(dateString.toString()).withZoneRetainFields(timeZone);
    } else {
      throw new IAE("Expected TIMESTAMP or DATE but got [%s]", typeName);
    }
  }

  /**
   * The inverse of {@link #jodaToCalciteTimestamp(DateTime, DateTimeZone)}.
   *
   * @param timestamp Calcite style timestamp
   * @param timeZone  session time zone
   *
   * @return joda timestamp, with time zone set to the session time zone
   */
  public static DateTime calciteTimestampToJoda(final long timestamp, final DateTimeZone timeZone)
  {
    return new DateTime(timestamp, DateTimeZone.UTC).withZoneRetainFields(timeZone);
  }

  /**
   * The inverse of {@link #jodaToCalciteDate(DateTime, DateTimeZone)}.
   *
   * @param date     Calcite style date
   * @param timeZone session time zone
   *
   * @return joda timestamp, with time zone set to the session time zone
   */
  public static DateTime calciteDateToJoda(final int date, final DateTimeZone timeZone)
  {
    return DateTimes.EPOCH.plusDays(date).withZoneRetainFields(timeZone);
  }

  public static String findUnusedPrefix(final String basePrefix, final Collection<String> strings)
  {
    if (!Iterables.any(strings, s -> s.startsWith(basePrefix))) {
      return basePrefix;
    }
    NavigableSet<String> current = Sets.newTreeSet(strings);
    String prefix = basePrefix;

    while (!isUnusedPrefix(prefix, current)) {
      prefix = "_" + prefix;
    }

    return prefix;
  }

  private static boolean isUnusedPrefix(final String prefix, final NavigableSet<String> strings)
  {
    // ":" is one character after "9"
    final NavigableSet<String> subSet = strings.subSet(prefix + "0", true, prefix + ":", false);
    return subSet.isEmpty();
  }

  public static String vcName(String prefix)
  {
    return makePrefixedName(prefix, "v");
  }

  public static String makePrefixedName(final String prefix, final String suffix)
  {
    return prefix + ":" + suffix;
  }

  public static RelDataType asRelDataType(ValueDesc columnType)
  {
    return asRelDataType(TYPE_FACTORY, columnType);
  }

  public static RelDataType asRelDataType(RelDataTypeFactory factory, ValueDesc columnType)
  {
    if (columnType.isDimension() || columnType.isMultiValued()) {
      return factory.createTypeWithNullability(DruidType.any(columnType, true), true);  // for intern type
    }
    switch (columnType.type()) {
      case STRING:
        return asRelDataType(factory, SqlTypeName.VARCHAR, true);
      case BOOLEAN:
        return asRelDataType(factory, SqlTypeName.BOOLEAN, true);
      case LONG:
        return asRelDataType(factory, SqlTypeName.BIGINT, true);
      case FLOAT:
        return asRelDataType(factory, SqlTypeName.FLOAT, true);
      case DOUBLE:
        return asRelDataType(factory, SqlTypeName.DOUBLE, true);
      case DATETIME:
        return asRelDataType(factory, DateTime.class);
      case COMPLEX:
        if (columnType.isStruct()) {
          final String[] description = columnType.getDescription();
          if (description == null) {
            RelDataType subType = factory.createSqlType(SqlTypeName.ANY);
            return factory.createTypeWithNullability(factory.createArrayType(subType, -1), true);
          }
          final List<String> fieldNames = Lists.newArrayList();
          final List<RelDataType> fieldTypes = Lists.newArrayList();
          for (int i = 1; i < description.length; i++) {
            int index = description[i].indexOf(':');
            fieldNames.add(description[i].substring(0, index));
            fieldTypes.add(asRelDataType(factory, ValueDesc.of(description[i].substring(index + 1))));
          }
          return factory.createTypeWithNullability(
              factory.createStructType(StructKind.PEEK_FIELDS, fieldTypes, fieldNames), true
          );
        } else if (columnType.isMap()) {
          final String[] description = columnType.getDescription();
          final RelDataType keyType = description != null ? asRelDataType(factory, ValueDesc.of(description[1]))
                                                          : asRelDataType(factory, SqlTypeName.VARCHAR);
          final RelDataType valueType = description != null ? asRelDataType(factory, ValueDesc.of(description[2]))
                                                            : factory.createSqlType(SqlTypeName.ANY);
//          final List<String> fieldNames = Arrays.asList(Row.MAP_KEY, Row.MAP_VALUE);
//          final List<RelDataType> fieldTypes = Arrays.asList(keyType, valueType);
//          return factory.createTypeWithNullability(
//              factory.createStructType(StructKind.PEEK_FIELDS, fieldTypes, fieldNames), true
//          );
          return factory.createTypeWithNullability(factory.createMapType(keyType, valueType), true);
        } else if (columnType.isArray()) {
          final RelDataType subType;
          final ValueDesc elementType = columnType.unwrapArray();
          if (elementType.isUnknown()) {
            subType = factory.createSqlType(SqlTypeName.ANY);
          } else {
            subType = asRelDataType(factory, elementType);
          }
          return factory.createTypeWithNullability(factory.createArrayType(subType, -1), true);
        } else if (columnType.isBitSet()) {
          return factory.createTypeWithNullability(
              factory.createArrayType(asRelDataType(factory, SqlTypeName.BOOLEAN), -1), true
          );
        } else if (ValueDesc.isGeometry(columnType)) {
          return asRelDataType(factory, columnType.asClass());
        }
        return DruidType.other(columnType);
      default:
        Class clazz = columnType.asClass();
        return clazz == null || clazz == Object.class ? TYPE_FACTORY.createUnknownType() : TYPE_FACTORY.createType(clazz);
    }
  }

  public static Class<?> sqlTypeNameJdbcToJavaClass(SqlTypeName typeName)
  {
    JDBCType jdbcType = JDBCType.valueOf(typeName.getJdbcOrdinal());
    switch (jdbcType) {
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
        return String.class;
      case NUMERIC:
      case DECIMAL:
        return BigDecimal.class;
      case BIT:
      case BOOLEAN:
        return Boolean.class;
      case TINYINT:
        return Byte.class;
      case SMALLINT:
        return Short.class;
      case INTEGER:
        return Integer.class;
      case BIGINT:
        return Long.class;
      case FLOAT:
      case REAL:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BINARY:
      case VARBINARY:
        return Byte[].class;
      case DATE:
        return Date.class;
      case TIME:
        return Time.class;
      case TIMESTAMP:
        return Timestamp.class;
      case STRUCT:
      case ARRAY:
        return List.class;
      default:
        return Object.class;
    }
  }
}
