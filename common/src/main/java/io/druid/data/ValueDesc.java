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
import com.google.common.base.Function;
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
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class ValueDesc implements Serializable, Cacheable
{
  private static final Interner<String> INTERNER = Interners.newStrongInterner();

  // primitives (should be conform with JsonValue of ValueType)
  public static final String STRING_TYPE = "string";
  public static final String FLOAT_TYPE = "float";
  public static final String DOUBLE_TYPE = "double";
  public static final String LONG_TYPE = "long";
  public static final String BOOLEAN_TYPE = "boolean";
  public static final String DATETIME_TYPE = "datetime";

  // non primitives
  public static final String MAP_TYPE = "map";
  public static final String LIST_TYPE = "list";
  public static final String INDEXED_ID_TYPE = "indexed";
  public static final String UNKNOWN_TYPE = "unknown";

  public static final String ARRAY_PREFIX = "array.";
  // aka. IndexedInts.WithLookup
  // this is return type of object selector which simulates dimension selector (used for some filter optimization)
  public static final String INDEXED_ID_PREFIX = "indexed.";
  // this is return type of object selector which can return element type or array of element type,
  // which is trait of dimension
  public static final String MULTIVALUED_PREFIX = "multivalued.";

  // prefix of dimension
  public static final String DIMENSION_PREFIX = "dimension.";

  // descriptive type
  public static final String DECIMAL_TYPE = "decimal";
  public static final String STRUCT_TYPE = "struct";

  static {
    INTERNER.intern(STRING_TYPE);
    INTERNER.intern(FLOAT_TYPE);
    INTERNER.intern(DOUBLE_TYPE);
    INTERNER.intern(LONG_TYPE);
    INTERNER.intern(BOOLEAN_TYPE);
    INTERNER.intern(DATETIME_TYPE);
    INTERNER.intern(MAP_TYPE);
    INTERNER.intern(LIST_TYPE);
    INTERNER.intern(INDEXED_ID_TYPE);
    INTERNER.intern(UNKNOWN_TYPE);
    INTERNER.intern(DECIMAL_TYPE);
    INTERNER.intern(STRUCT_TYPE);
  }

  // primitives
  public static ValueDesc STRING = new ValueDesc(ValueType.STRING);
  public static ValueDesc BOOLEAN = new ValueDesc(ValueType.BOOLEAN);
  public static ValueDesc FLOAT = new ValueDesc(ValueType.FLOAT);
  public static ValueDesc DOUBLE = new ValueDesc(ValueType.DOUBLE);
  public static ValueDesc LONG = new ValueDesc(ValueType.LONG);
  public static ValueDesc DATETIME = new ValueDesc(ValueType.DATETIME);

  // dimension
  public static ValueDesc DIM_STRING = new ValueDesc(DIMENSION_PREFIX + STRING_TYPE);

  // internal types
  public static ValueDesc MAP = new ValueDesc(MAP_TYPE, Map.class);
  public static ValueDesc LIST = new ValueDesc(LIST_TYPE);
  public static ValueDesc INDEXED_ID = new ValueDesc(INDEXED_ID_TYPE);

  // from expression
  public static ValueDesc DECIMAL = new ValueDesc(DECIMAL_TYPE, BigDecimal.class);
  public static ValueDesc STRUCT = new ValueDesc(STRUCT_TYPE, Object[].class);
  public static ValueDesc UNKNOWN = new ValueDesc(UNKNOWN_TYPE);

  public static ValueDesc STRING_ARRAY = new ValueDesc(ARRAY_PREFIX + STRING_TYPE);
  public static ValueDesc LONG_ARRAY = new ValueDesc(ARRAY_PREFIX + LONG_TYPE);
  public static ValueDesc FLOAT_ARRAY = new ValueDesc(ARRAY_PREFIX + FLOAT_TYPE);
  public static ValueDesc DOUBLE_ARRAY = new ValueDesc(ARRAY_PREFIX + DOUBLE_TYPE);

  public static ValueDesc SHAPE = new ValueDesc("SHAPE");
  public static ValueDesc GEOMETRY = new ValueDesc("GEOMETRY");
  public static ValueDesc OGC_GEOMETRY = new ValueDesc("OGC_GEOMETRY");
  public static ValueDesc INTERVAL = new ValueDesc("INTERVAL", Interval.class);

  public static ValueDesc ofArray(ValueDesc valueType)
  {
    return ofArray(valueType.typeName);
  }

  public static ValueDesc ofArray(ValueType valueType)
  {
    return ofArray(valueType.getName());
  }

  public static ValueDesc ofList(ValueDesc valueType)
  {
    return ofList(valueType.typeName);
  }

  public static ValueDesc ofArray(String typeName)
  {
    return ValueDesc.of(ARRAY_PREFIX + typeName);
  }

  public static ValueDesc ofList(String element)
  {
    return ValueDesc.of(LIST_TYPE + "(" + element + ")");
  }

  public static ValueDesc ofStruct(String elements)
  {
    return of(STRUCT_TYPE + "(" + elements + ")");
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
    return of(builder.toString());
  }

  public static ValueDesc ofDimension(ValueType valueType)
  {
    Preconditions.checkArgument(valueType.isPrimitive(), "complex type dimension is not allowed");
    return of(DIMENSION_PREFIX + valueType.getName());
  }

  public static ValueDesc ofDecimal(BigDecimal decimal)
  {
    return ValueDesc.of(DECIMAL_TYPE + "(" + decimal.precision() + "," + decimal.scale() + ")");
  }

  public static ValueDesc ofMultiValued(ValueType valueType)
  {
    return ValueDesc.of(MULTIVALUED_PREFIX + valueType.getName());
  }

  public static ValueDesc ofMultiValued(ValueDesc valueType)
  {
    return ValueDesc.of(MULTIVALUED_PREFIX + valueType.typeName());
  }

  public static ValueDesc ofIndexedId(ValueType valueType)
  {
    return ValueDesc.of(INDEXED_ID_PREFIX + valueType.getName());
  }

  public static boolean isArray(ValueDesc valueType)
  {
    return isArray(valueType.typeName);
  }

  public static boolean isArray(String typeName)
  {
    return isPrefixed(typeName, ARRAY_PREFIX);
  }

  public static boolean isDimension(ValueDesc valueType)
  {
    return valueType != null && isDimension(valueType.typeName);
  }

  public static boolean isMultiValued(ValueDesc valueType)
  {
    return valueType != null && isPrefixed(valueType.typeName, MULTIVALUED_PREFIX);
  }

  public static boolean isDimension(String typeName)
  {
    return isPrefixed(typeName, DIMENSION_PREFIX);
  }

  public static boolean isIndexedId(ValueDesc valueType)
  {
    return isIndexedId(valueType.typeName);
  }

  public static boolean isIndexedId(String typeName)
  {
    return typeName.equals(INDEXED_ID_TYPE) || isPrefixed(typeName, INDEXED_ID_PREFIX);
  }

  private static boolean isPrefixed(String typeName, String prefix)
  {
    return typeName != null && typeName.toLowerCase().startsWith(prefix);
  }

  public static ValueDesc elementOfArray(ValueDesc valueType)
  {
    return valueType == null ? null : elementOfArray(valueType.typeName, null);
  }

  public static ValueDesc elementOfArray(String typeName)
  {
    return typeName == null ? null : elementOfArray(typeName, null);
  }

  public static ValueDesc elementOfArray(String typeName, ValueDesc defaultType)
  {
    String elementType = typeName == null ? null : subElementOf(typeName, ARRAY_PREFIX);
    return elementType != null ? of(elementType) : defaultType;
  }

  public static ValueDesc elementOfArray(ValueDesc valueType, ValueDesc defaultType)
  {
    String elementType = valueType == null ? null : subElementOf(valueType.typeName, ARRAY_PREFIX);
    return elementType != null ? of(elementType) : defaultType;
  }

  private static String subElementOf(String typeName, String prefix)
  {
    return isPrefixed(typeName, prefix) ? typeName.substring(prefix.length()) : null;
  }

  public static ValueDesc subElementOf(ValueDesc valueType, ValueDesc defaultType)
  {
    String typeName = valueType == null ? null : subElementOf(valueType.typeName);
    return typeName == null ? defaultType : ValueDesc.of(typeName);
  }

  public static String headElementOf(String typeName)
  {
    int index = typeName.indexOf('.');
    return index < 0 ? null : typeName.substring(0, index);
  }

  public static String subElementOf(String typeName)
  {
    int index = typeName.indexOf('.');
    return index < 0 ? null : typeName.substring(index + 1);
  }

  public static ValueDesc subElementOf(ValueDesc valueType)
  {
    if (valueType != null) {
      String elementType = subElementOf(valueType.typeName);
      if (elementType != null) {
        return ValueDesc.of(elementType);
      }
    }
    return null;
  }

  public static ValueType typeOfDimension(ValueDesc valueType)
  {
    Preconditions.checkArgument(isDimension(valueType));
    return ValueType.of(subElementOf(valueType.typeName()));
  }

  public static ValueDesc assertPrimitive(ValueDesc valueDesc)
  {
    Preconditions.checkArgument(valueDesc.isPrimitive(), "should be primitive type but " + valueDesc);
    return valueDesc;
  }

  public static ValueDesc assertNumeric(ValueDesc valueDesc)
  {
    Preconditions.checkArgument(valueDesc.isNumeric(), "should be numeric type but " + valueDesc);
    return valueDesc;
  }

  public static Function<String, ValueDesc> resolving(final TypeResolver resolver)
  {
    return new Function<String, ValueDesc>()
    {
      @Override
      public ValueDesc apply(String input)
      {
        return resolver.resolve(input);
      }
    };
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
      return type2;
    }
    if (type2 == null) {
      return type1;
    }
    if (type1.equals(type2)) {
      return type1;
    }
    if (type1.isDecimal() && type2.isDecimal()) {
      return commonDecimal(type1, type2);
    }
    if (type1.isDecimal() && type2.isNumeric()) {
      return type1;
    }
    if (type1.isNumeric() && type2.isDecimal()) {
      return type2;
    }
    if (type1.isNumeric() && type2.isNumeric()) {
      return ValueDesc.DOUBLE;
    }
    if (type1.isStringOrDimension() && type2.isStringOrDimension()) {
      return ValueDesc.STRING;
    }
    return ValueDesc.UNKNOWN;
  }

  // see DecimalMetricSerde
  private static ValueDesc commonDecimal(ValueDesc type1, ValueDesc type2)
  {
    String[] desc1 = TypeUtils.splitDescriptiveType(type1.typeName);
    String[] desc2 = TypeUtils.splitDescriptiveType(type2.typeName);

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

    return ValueDesc.of(DECIMAL_TYPE + "(" + precision + "," + s1 + "," + mode + ")");
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

  // complex types are case sensitive (same with serde-name) but primitive types are not.. hate this
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
    int index = typeName.indexOf('.');
    if (index > 0) {
      return ValueDesc.of(typeName.substring(index + 1));
    }
    throw new IllegalStateException("does not have sub element");
  }

  public boolean hasSubElement()
  {
    return !type.isPrimitive() && typeName.indexOf('.') >= 0;
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
      case LONG_TYPE: return LONG;
      case BOOLEAN_TYPE: return BOOLEAN;
      case DATETIME_TYPE: return DATETIME;
      case DECIMAL_TYPE: return DECIMAL;
      case STRUCT_TYPE: return STRUCT;
      case MAP_TYPE: return MAP;
      case LIST_TYPE: return LIST;
    }
    return new ValueDesc(typeName, clazz);
  }

  public static boolean isMap(ValueDesc type)
  {
    return type != null && type.isMap();
  }

  public static boolean isString(ValueDesc type)
  {
    return type != null && type.isString();
  }

  public static boolean isStringOrDimension(ValueDesc type)
  {
    return type != null && (type.type == ValueType.STRING || ValueDesc.isDimension(type));
  }

  public static boolean isPrimitive(ValueDesc type)
  {
    return type != null && type.isPrimitive();
  }

  public static boolean isType(ValueDesc type, String typeName)
  {
    return type != null && typeName.equals(type.typeName);
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
    return typeName == DECIMAL_TYPE || typeName.startsWith(DECIMAL_TYPE);
  }

  public boolean isMap()
  {
    return typeName == MAP_TYPE || typeName.startsWith(MAP_TYPE);
  }

  public boolean isString()
  {
    return type == ValueType.STRING;
  }

  public boolean isDimension()
  {
    return isDimension(typeName);
  }

  public boolean isArray()
  {
    return isArray(typeName);
  }

  public boolean isStringOrDimension()
  {
    return isString() || isDimension();
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
    return typeName == STRUCT_TYPE || typeName.toLowerCase().startsWith(STRUCT_TYPE);
  }

  public boolean isList()
  {
    return typeName == LIST_TYPE || typeName.toLowerCase().startsWith(LIST_TYPE);
  }

  public boolean isUnknown()
  {
    return typeName == UNKNOWN_TYPE || typeName.equals(UNKNOWN_TYPE);
  }

  public boolean hasDescription()
  {
    return typeName.endsWith(")") && typeName.indexOf('(') > 0;
  }

  public String[] getDescription()
  {
    return TypeUtils.splitDescriptiveType(typeName);
  }

  public Object cast(Object value)
  {
    return clazz == null ? type.cast(value) : clazz.cast(value);
  }

  public Class asClass()
  {
    if (clazz != null) {
      return clazz;
    }
    if (isPrimitive()) {
      return type.classOfObject();
    }
    String type = typeName.toLowerCase();
    if (type.startsWith(STRUCT_TYPE) || type.startsWith(LIST_TYPE)) {
      return List.class;
    } else if (type.startsWith(MAP_TYPE)) {
      return Map.class;
    } else if (type.startsWith(DECIMAL_TYPE)) {
      return BigDecimal.class;
    } else if (type.startsWith(ARRAY_PREFIX)) {
      Class elementType = ValueDesc.of(subElementOf(type)).asClass();
      return Array.newInstance(elementType, 0).getClass();
    }
    return Object.class;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return type.isPrimitive() ? builder.append(type) : builder.append(typeName);
  }
}
