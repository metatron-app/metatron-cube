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

package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import org.joda.time.Interval;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ValueDesc implements Serializable, Cacheable
{
  private static final Interner<String> INTERNER = Interners.newStrongInterner();

  // primitives (should be conformed with JsonValue of ValueType)
  public static final String STRING_TYPE = "string";
  public static final String FLOAT_TYPE = "float";
  public static final String DOUBLE_TYPE = "double";
  public static final String INT_TYPE = "int";
  public static final String LONG_TYPE = "long";
  public static final String BOOLEAN_TYPE = "boolean";
  public static final String DATETIME_TYPE = "datetime";

  // non primitives
  private static final String INDEXED_ID_TYPE = "indexed";
  private static final String BITSET_TYPE = "bitset";
  private static final String UNKNOWN_TYPE = "unknown";

  // aka. IndexedInts.WithLookup
  // this is return type of object selector which simulates dimension selector (used for some filter optimization)
  private static final String INDEXED_ID_PREFIX = "indexed.";
  // this is return type of object selector which can return element type or array of element type,
  // which is trait of dimension
  private static final String MV_TYPE = "mv";
  private static final String MV_PREFIX = MV_TYPE + ".";

  public static final String DIMENSION_TYPE = "dimension";
  public static final String DIMENSION_PREFIX = DIMENSION_TYPE + ".";

  // descriptive type
  public static final String MAP_TYPE = "map";
  public static final String ARRAY_TYPE = "array";
  private static final String LEGACY_ARRAY_PREFIX = "array.";
  private static final String GENERIC_ARRAY_PREFIX = "array(";

  public static final String DECIMAL_TYPE = "decimal";
  public static final String DECIMAL_TYPE_PREFIX = DECIMAL_TYPE + "(";

  public static final String STRUCT_TYPE = "struct";
  public static final String STRUCT_TYPE_PREFIX = STRUCT_TYPE + "(";

  public static final String ENUM_TYPE = "enum";
  public static final String ENUM_TYPE_PREFIX = ENUM_TYPE + "(";

  public static final String TAG_TYPE = "tag";

  public static final String STRING_MV_TYPE = MV_PREFIX + STRING_TYPE;

  public static final String STRING_DIMENSION_TYPE = DIMENSION_PREFIX + STRING_TYPE;
  public static final String STRING_MV_DIMENSION_TYPE = DIMENSION_PREFIX + STRING_MV_TYPE;

  static {
    INTERNER.intern(STRING_TYPE);
    INTERNER.intern(FLOAT_TYPE);
    INTERNER.intern(DOUBLE_TYPE);
    INTERNER.intern(LONG_TYPE);
    INTERNER.intern(BOOLEAN_TYPE);
    INTERNER.intern(DATETIME_TYPE);
    INTERNER.intern(MAP_TYPE);
    INTERNER.intern(ARRAY_TYPE);
    INTERNER.intern(INDEXED_ID_TYPE);
    INTERNER.intern(BITSET_TYPE);
    INTERNER.intern(UNKNOWN_TYPE);
    INTERNER.intern(DECIMAL_TYPE);
    INTERNER.intern(STRUCT_TYPE);
  }

  // primitives
  public static final ValueDesc STRING = new ValueDesc(ValueType.STRING);
  public static final ValueDesc BOOLEAN = new ValueDesc(ValueType.BOOLEAN);
  public static final ValueDesc FLOAT = new ValueDesc(ValueType.FLOAT);
  public static final ValueDesc DOUBLE = new ValueDesc(ValueType.DOUBLE);
  public static final ValueDesc LONG = new ValueDesc(ValueType.LONG);
  public static final ValueDesc DATETIME = new ValueDesc(ValueType.DATETIME);

  // dimension
  public static final ValueDesc DIM_STRING = new ValueDesc(STRING_DIMENSION_TYPE);
  public static final ValueDesc DIM_MV_STRING = new ValueDesc(STRING_MV_DIMENSION_TYPE);

  // mv
  public static final ValueDesc MV = new ValueDesc(MV_TYPE);
  public static final ValueDesc MV_STRING = new ValueDesc(STRING_MV_TYPE);

  // internal types
  public static final ValueDesc MAP = new ValueDesc(MAP_TYPE, Map.class);
  public static final ValueDesc ARRAY = new ValueDesc(ARRAY_TYPE, List.class);
  public static final ValueDesc INDEXED_ID = new ValueDesc(INDEXED_ID_TYPE);

  // from expression
  public static final ValueDesc DECIMAL = new ValueDesc(DECIMAL_TYPE, BigDecimal.class);
  public static final ValueDesc STRUCT = new ValueDesc(STRUCT_TYPE, List.class);
  public static final ValueDesc UNKNOWN = new ValueDesc(UNKNOWN_TYPE);

  // shortcuts
  public static final ValueDesc ENUM = new ValueDesc(ENUM_TYPE);
  public static final ValueDesc TAG = new ValueDesc(TAG_TYPE);

  // legacy arrays
  public static final ValueDesc STRING_ARRAY = new ValueDesc(LEGACY_ARRAY_PREFIX + STRING_TYPE);
  public static final ValueDesc LONG_ARRAY = new ValueDesc(LEGACY_ARRAY_PREFIX + LONG_TYPE);
  public static final ValueDesc FLOAT_ARRAY = new ValueDesc(LEGACY_ARRAY_PREFIX + FLOAT_TYPE);
  public static final ValueDesc DOUBLE_ARRAY = new ValueDesc(LEGACY_ARRAY_PREFIX + DOUBLE_TYPE);

  public static final ValueDesc GEOMETRY = new ValueDesc("geometry");
  public static final ValueDesc OGC_GEOMETRY = new ValueDesc("ogc_geometry");
  public static final ValueDesc SHAPE = new ValueDesc("shape");
  public static final ValueDesc INTERVAL = new ValueDesc("interval", Interval.class);
  public static final ValueDesc BITSET = new ValueDesc(BITSET_TYPE, BitSet.class);

  public static ValueDesc ofArray(String typeName)
  {
    return ofArray(ValueDesc.of(typeName));
  }

  public static ValueDesc ofArray(ValueDesc valueType)
  {
    if (valueType.isPrimitive()) {
      return ofArray(valueType.type);
    }
    return ValueDesc.of(String.format("%s(%s)", ARRAY_TYPE, valueType.typeName));
  }

  public static ValueDesc ofArray(ValueType valueType)
  {
    return ValueDesc.of(LEGACY_ARRAY_PREFIX + valueType.getName());
  }

  public static ValueDesc ofMap(ValueDesc key, ValueDesc value)
  {
    return ValueDesc.of(String.format("%s(%s,%s)", MAP_TYPE, key.typeName, value.typeName), Map.class);
  }

  public static ValueDesc ofDecimal(BigDecimal decimal)
  {
    return decimal == null ? DECIMAL : ofDecimal(decimal.precision(), decimal.scale(), null);
  }

  public static ValueDesc ofDecimal(int precision, int scale, RoundingMode roundingMode)
  {
    if (roundingMode == null) {
      return ValueDesc.of(String.format("%s(%d,%d)", DECIMAL_TYPE, precision, scale));
    } else {
      return ValueDesc.of(String.format("%s(%d,%d,%s)", DECIMAL_TYPE, precision, scale, roundingMode));
    }
  }

  public static ValueDesc ofStruct(String elements)
  {
    return of(String.format("%s(%s)", STRUCT_TYPE, elements));
  }

  public static ValueDesc ofStruct(String[] names, ValueDesc[] types)
  {
    Preconditions.checkArgument(names.length == types.length);
    StringBuilder builder = new StringBuilder();
    builder.append(STRUCT_TYPE).append('(');
    for (int i = 0; i < names.length; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(names[i]).append(':').append(types[i] == null ? UNKNOWN_TYPE : types[i]);
    }
    builder.append(')');
    return of(builder.toString(), List.class);
  }

  public static ValueDesc ofDimension(ValueDesc valueDesc, boolean mv)
  {
    if (valueDesc.isString()) {
      return mv ? DIM_MV_STRING : DIM_STRING;
    }
    String prefix = mv ? DIMENSION_PREFIX + MV_PREFIX : DIMENSION_PREFIX;
    return ValueDesc.of(prefix + valueDesc.typeName());
  }

  public static ValueDesc ofDimension(ValueType valueType, boolean mv)
  {
    Preconditions.checkArgument(valueType.isPrimitive(), "complex type dimension is not allowed");
    if (valueType == ValueType.STRING) {
      return mv ? DIM_MV_STRING : DIM_STRING;
    }
    String prefix = mv ? DIMENSION_PREFIX + MV_PREFIX : DIMENSION_PREFIX;
    return ValueDesc.of(prefix + valueType.getName());
  }

  public static ValueDesc ofMultiValued(ValueType valueType)
  {
    return valueType == ValueType.STRING ? MV_STRING : ValueDesc.of(MV_PREFIX + valueType.getName());
  }

  public static ValueDesc ofMultiValued(ValueDesc valueType)
  {
    return valueType.isString() ? MV_STRING : ValueDesc.of(MV_PREFIX + valueType.typeName());
  }

  public static ValueDesc ofIndexedId(ValueType valueType)
  {
    return ValueDesc.of(INDEXED_ID_PREFIX + valueType.getName());
  }

  public static boolean isString(ValueDesc valueDesc)
  {
    return valueDesc != null && valueDesc.isString();
  }

  public static boolean isGeometry(ValueDesc valueType)
  {
    return isPrefixed(valueType.typeName, GEOMETRY.typeName);
  }

  public static boolean isOGCGeometry(ValueDesc valueType)
  {
    return isPrefixed(valueType.typeName, OGC_GEOMETRY.typeName);
  }

  public static boolean isDimension(ValueDesc valueType)
  {
    return valueType != null && valueType.isDimension();
  }

  public static boolean isIndexedId(ValueDesc valueType)
  {
    return valueType != null && isIndexedId(valueType.typeName);
  }

  public static boolean isIndexedId(String typeName)
  {
    return typeName.equals(INDEXED_ID_TYPE) || isPrefixed(typeName, INDEXED_ID_PREFIX);
  }

  private static boolean isPrefixed(String typeName, String prefix)
  {
    return typeName != null && typeName.toLowerCase().startsWith(prefix);
  }

  public static String subElementOf(String typeName)
  {
    int index = typeName.indexOf('.');
    return index < 0 ? null : typeName.substring(index + 1);
  }

  public static ValueType typeOfDimension(ValueDesc valueType)
  {
    Preconditions.checkArgument(isDimension(valueType));
    return ValueType.of(subElementOf(valueType.typeName()));
  }

  public static ValueDesc assertPrimitive(ValueDesc valueDesc)
  {
    Preconditions.checkArgument(valueDesc.isPrimitive(), "should be primitive type but %s", valueDesc);
    return valueDesc;
  }

  public static String merge(String typeName0, String typeName1)
  {
    if (isDimension(typeName0) && isDimension(typeName1)) {
      ValueDesc type0 = ValueDesc.of(typeName0).unwrapDimension();
      ValueDesc type1 = ValueDesc.of(typeName1).unwrapDimension();
      boolean mv0 = type0.isMultiValued();
      boolean mv1 = type1.isMultiValued();
      if (!mv0 && !mv1) {
        return type0.equals(type1) ? typeName0.length() > typeName1.length() ? typeName0 : typeName1 : null;
      } else if (mv0 && !mv1) {
        return type0.subElement(ValueDesc.STRING).equals(type1) ? typeName0 : null;
      } else if (!mv0 && mv1) {
        return type0.equals(type1.subElement(ValueDesc.STRING)) ? typeName1 : null;
      } else {
        return type0.subElement(ValueDesc.STRING).equals(type1.subElement(ValueDesc.STRING)) ? typeName0 : null;
      }
    }
    return typeName0.equals(typeName1) ? typeName0 : null;
  }

  public static ValueDesc toCommonType(Iterable<ValueDesc> valueDescs, ValueDesc notFound)
  {
    ValueDesc current = null;
    for (ValueDesc valueDesc : valueDescs) {
      current = toCommonType(current, valueDesc);
    }
    return current == null || current.isUnknown() ? notFound : current;
  }

  public static ValueDesc toCommonType(Expr type1, Expr type2)
  {
    if (Evals.isExplicitNull(type1)) {
      return Evals.isExplicitNull(type2) ? null : type2.returns();
    } else if (Evals.isExplicitNull(type2)) {
      return type1.returns();
    }
    return toCommonType(type1.returns(), type2.returns());
  }

  public static ValueDesc toCommonType(ValueDesc type1, Expr type2)
  {
    return Evals.isExplicitNull(type2) ? type1 : toCommonType(type1, type2.returns());
  }

  public static ValueDesc toCommonType(ValueDesc type1, ValueDesc type2)
  {
    if (type1 == null) {
      return type2 == null ? null : type2.unwrapDimension();
    }
    if (type2 == null) {
      return type1.unwrapDimension();
    }
    type1 = type1.unwrapDimension();
    type2 = type2.unwrapDimension();
    if (type1.equals(type2)) {
      return type1;
    }

    boolean decimal1 = type1.isDecimal();
    boolean decimal2 = type2.isDecimal();

    if (decimal1 && decimal2) {
      return commonDecimal(type1, type2);
    }
    if (decimal1 && type2.isPrimitiveNumeric()) {
      return type1;
    }
    if (type1.isPrimitiveNumeric() && decimal2) {
      return type2;
    }
    if (type1.isPrimitiveNumeric() && type2.isPrimitiveNumeric()) {
      return ValueDesc.DOUBLE;
    }
    return ValueDesc.UNKNOWN;
  }

  // see DecimalMetricSerde
  private static ValueDesc commonDecimal(ValueDesc type1, ValueDesc type2)
  {
    String[] desc1 = type1.getDescription();
    String[] desc2 = type2.getDescription();

    int p1 = desc1.length > 1 ? Integer.valueOf(desc1[1]) : 18;
    int p2 = desc2.length > 1 ? Integer.valueOf(desc2[1]) : 18;

    int s1 = desc1.length > 2 ? Integer.valueOf(desc1[2]) : 0;
    int s2 = desc2.length > 2 ? Integer.valueOf(desc2[2]) : 0;

    String mode = null;
    if (desc1.length > 3 || desc2.length > 3) {
      String mode1 = desc1.length > 3 && !Strings.isNullOrEmpty(desc1[3]) ? desc1[3] : "DOWN";
      String mode2 = desc2.length > 3 && !Strings.isNullOrEmpty(desc2[3]) ? desc2[3] : "DOWN";
      Preconditions.checkArgument(Objects.equals(mode1, mode2), "different rounding mode");
      mode = mode1;
    }
    int scale = Math.max(s1, s2);
    int precision = scale + Math.max(p1 - s1, p2 - s2) + 1;

    return ofDecimal(precision, scale, mode == null ? null : RoundingMode.valueOf(mode));
  }

  public static String toTypeString(ValueDesc desc)
  {
    if (desc == null || desc.isUnknown()) {
      return STRING_TYPE;
    }
    if (desc.isDimension()) {
      return desc.subElement(ValueDesc.STRING).typeName;
    }
    String typeName = desc.typeName();
    final int index = typeName.indexOf('(');
    if (index > 0 && typeName.indexOf(index + 1, ')') > 0) {
      typeName = typeName.substring(0, index);  // remove description
    }
    return typeName.toLowerCase();
  }

  public static ValueDesc fromTypeString(String name)
  {
    if (Strings.isNullOrEmpty(name)) {
      return ValueDesc.STRING;
    }
    switch (name.toUpperCase()) {
      case "BOOLEAN":
        return ValueDesc.BOOLEAN;
      case "FLOAT":
        return ValueDesc.FLOAT;
      case "DOUBLE":
        return ValueDesc.DOUBLE;
      case "BYTE":
      case "SHORT":
      case "INT":
      case "INTEGER":
      case "LONG":
      case "BIGINT":
        return ValueDesc.LONG;
    }
    return ValueDesc.of(name);
  }

  public static boolean isSameCategory(ValueDesc type1, ValueDesc type2)
  {
    if (type1.isPrimitive()) {
      return type1.equals(type2);
    }
    if (type1.hasSubElement()) {
      ValueDesc category1 = type1.categoryType();
      ValueDesc element1 = type1.subElement();
      if (type2.hasSubElement()) {
        return category1.equals(type2.categoryType()) && element1.equals(type2.subElement());
      } else {
        return category1.equals(type2);
      }
    }
    if (type2.hasSubElement()) {
      return type1.equals(type2.categoryType());
    }
    return type1.equals(type2);
  }

  private final ValueType type;
  private final String typeName;
  private final Class clazz;

  private ValueDesc(ValueType primitive)
  {
    Preconditions.checkArgument(primitive.isPrimitive(), "should be primitive type");
    this.type = primitive;
    this.typeName = primitive.getName();
    this.clazz = null;
  }

  private ValueDesc(String typeName)
  {
    this(typeName, null);
  }

  private ValueDesc(String typeName, Class clazz)
  {
    this.type = ValueType.of(Preconditions.checkNotNull(typeName, "typeName cannot be null"));
    this.typeName = type.isPrimitive() ? type.getName() : INTERNER.intern(normalize(typeName));
    this.clazz = clazz;
  }

  // complex types are case-sensitive (same with serde-name) but primitive types are not.. hate that
  private String normalize(String typeName)
  {
    ValueType valueType = ValueType.of(typeName);
    if (valueType.isPrimitive()) {
      return valueType.getName();
    }
    int index = typeName.lastIndexOf('.');
    if (index < 0) {
      return typeName;
    }
    // todo fix this
    return typeName.substring(0, index + 1) + normalize(typeName.substring(index + 1));
  }

  public ValueType type()
  {
    return type;
  }

  @JsonValue
  public String typeName()
  {
    return typeName;
  }

  public ValueDesc categoryType()
  {
    int index = typeName.indexOf('.');
    if (index > 0) {
      return ValueDesc.of(typeName.substring(0, index));
    }
    throw new IllegalStateException("does not have sub element");
  }

  public ValueDesc subElement()
  {
    return Preconditions.checkNotNull(subElement(null));
  }

  public ValueDesc subElement(ValueDesc defaultType)
  {
    if (this == DIM_STRING || this == MV_STRING || this == STRING_ARRAY) {
      return ValueDesc.STRING;
    } else if (this == DIM_MV_STRING) {
      return ValueDesc.MV_STRING;
    } else if (this == FLOAT_ARRAY) {
      return ValueDesc.FLOAT;
    } else if (this == DOUBLE_ARRAY) {
      return ValueDesc.DOUBLE;
    } else if (this == LONG_ARRAY) {
      return ValueDesc.LONG;
    }
    int index = typeName.indexOf('.');
    if (index > 0) {
      return ValueDesc.of(typeName.substring(index + 1));
    }
    return defaultType;
  }

  public ValueDesc unwrapArray()
  {
    return unwrapArray(ValueDesc.UNKNOWN);
  }

  public ValueDesc unwrapArray(ValueDesc unknown)
  {
    if (isLegacyArray()) {
      return subElement(unknown);
    }
    String[] description = TypeUtils.splitDescriptiveType(typeName);
    return description == null ? unknown : ValueDesc.of(description[1]);
  }

  private boolean hasSubElement()
  {
    if (type.isPrimitive()) {
      return false;
    }
    int dx = typeName.indexOf('.');
    int lx = typeName.indexOf('(');
    return dx >= 0 && (lx < 0 || dx < lx);
  }

  public ValueDesc unwrapDimension()
  {
    return DIMENSION_TYPE.equals(typeName) ? ValueDesc.STRING : typeName.startsWith(DIMENSION_PREFIX) ? subElement() : this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ValueDesc other = (ValueDesc) o;

    if (type.isPrimitive()) {
      return type == other.type;
    }

    return typeName.equals(other.typeName);
  }

  @Override
  public int hashCode()
  {
    return typeName.hashCode();
  }

  @Override
  public String toString()
  {
    return typeName;
  }

  @JsonCreator
  public static ValueDesc of(String typeName)
  {
    return of(typeName, null);
  }

  public static ValueDesc of(ValueType valueType)
  {
    if (valueType == null) {
      return ValueDesc.UNKNOWN;
    }
    switch (valueType) {
      case STRING: return STRING;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case LONG: return LONG;
      case BOOLEAN: return BOOLEAN;
      case DATETIME: return DATETIME;
    }
    return new ValueDesc(valueType);
  }

  public static ValueDesc of(String typeName, Class clazz)
  {
    if (typeName == null) {
      return null;
    }
    switch (typeName) {
      case STRING_TYPE: return STRING;
      case FLOAT_TYPE: return FLOAT;
      case DOUBLE_TYPE: return DOUBLE;
      case INT_TYPE: return LONG;
      case LONG_TYPE: return LONG;
      case BOOLEAN_TYPE: return BOOLEAN;
      case DATETIME_TYPE: return DATETIME;
      case DECIMAL_TYPE: return DECIMAL;
      case STRUCT_TYPE: return STRUCT;
      case MAP_TYPE: return MAP;
      case ARRAY_TYPE: return ARRAY;
      case STRING_DIMENSION_TYPE: return DIM_STRING;
    }
    return new ValueDesc(typeName, clazz);
  }

  public static boolean isMap(ValueDesc type)
  {
    return type != null && type.isMap();
  }

  public static boolean isPrimitive(ValueDesc type)
  {
    return type != null && type.isPrimitive();
  }

  public Comparator comparator()
  {
    return type.comparator();
  }

  public boolean isPrimitive()
  {
    return type.isPrimitive();
  }

  public boolean isPrimitiveNumeric()
  {
    return type.isNumeric();
  }

  public boolean isBoolean()
  {
    return type == ValueType.BOOLEAN;
  }

  public boolean isNumeric()
  {
    return type.isNumeric() || isDecimal();
  }

  public boolean isDecimal()
  {
    return DECIMAL_TYPE.equals(typeName) || typeName.startsWith(DECIMAL_TYPE_PREFIX);
  }

  public boolean isMap()
  {
    return MAP_TYPE.equals(typeName) || typeName.startsWith(MAP_TYPE);
  }

  public boolean isString()
  {
    return type == ValueType.STRING;
  }

  public boolean isDimension()
  {
    return isDimension(typeName);
  }

  private static boolean isDimension(String typeName)
  {
    return DIMENSION_TYPE.equals(typeName) || typeName.startsWith(DIMENSION_PREFIX);
  }

  public boolean isStringOrDimension()
  {
    return isString() || isDimension();
  }

  public boolean isArrayOrStruct()
  {
    return isArray() || isStruct();
  }

  public boolean isFloat()
  {
    return type == ValueType.FLOAT;
  }

  public boolean isDouble()
  {
    return type == ValueType.DOUBLE;
  }

  public boolean isLong()
  {
    return type == ValueType.LONG;
  }

  public boolean isDateTime()
  {
    return type == ValueType.DATETIME;
  }

  public boolean isStruct()
  {
    return this == STRUCT || STRUCT_TYPE.equals(typeName) || typeName.startsWith(STRUCT_TYPE_PREFIX);
  }

  public boolean isArray()
  {
    return isLegacyArray() || isGenericArray();
  }

  public boolean isLegacyArray()
  {
    return this == ARRAY || ARRAY_TYPE.equals(typeName) || typeName.startsWith(LEGACY_ARRAY_PREFIX);
  }

  public boolean isGenericArray()
  {
    return typeName.startsWith(GENERIC_ARRAY_PREFIX);
  }

  public boolean isBitSet()
  {
    return this == BITSET || BITSET_TYPE.equals(typeName);
  }

  public boolean isMultiValued()
  {
    return MV.equals(this) || typeName.startsWith(MV_PREFIX);
  }

  public boolean isUnknown()
  {
    return this == UNKNOWN || UNKNOWN_TYPE.equals(typeName);
  }

  public boolean isEnum()
  {
    return this == ENUM || ENUM_TYPE.equals(typeName) || typeName.startsWith(ENUM_TYPE_PREFIX);
  }

  public boolean isTag()
  {
    return this == TAG || TAG_TYPE.equals(typeName);
  }

  public String[] getDescription()
  {
    return TypeUtils.splitDescriptiveType(typeName);
  }

  public Object cast(Object value)
  {
    return clazz == null ? type.cast(value) : clazz.cast(value);
  }

  public Class<?> asClass()
  {
    if (clazz != null) {
      return clazz;
    }
    if (isPrimitive()) {
      return type.classOfObject();
    }
    final String type = typeName.toLowerCase();
    if (type.startsWith(STRUCT_TYPE) || type.startsWith(ARRAY_TYPE)) {
      return List.class;
    } else if (type.startsWith(MAP_TYPE)) {
      return Map.class;
    } else if (type.startsWith(DECIMAL_TYPE)) {
      return BigDecimal.class;
    } else if (type.startsWith(ENUM_TYPE) || type.equals(TAG_TYPE)) {
      return String.class;
    }
    return Object.class;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return type.isPrimitive() ? builder.append(type) : builder.append(typeName);
  }
}
