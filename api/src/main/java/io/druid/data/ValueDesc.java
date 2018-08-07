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

package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;

/**
 */
public class ValueDesc implements Serializable
{
  // primitives (should be conform with JsonValue of ValueType)
  public static final String STRING_TYPE = "string";
  public static final String FLOAT_TYPE = "float";
  public static final String DOUBLE_TYPE = "double";
  public static final String LONG_TYPE = "long";
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

  // primitives
  public static ValueDesc STRING = new ValueDesc(ValueType.STRING);
  public static ValueDesc FLOAT = new ValueDesc(ValueType.FLOAT);
  public static ValueDesc DOUBLE = new ValueDesc(ValueType.DOUBLE);
  public static ValueDesc LONG = new ValueDesc(ValueType.LONG);
  public static ValueDesc DATETIME = new ValueDesc(ValueType.DATETIME);

  // dimension
  public static ValueDesc DIM_STRING = new ValueDesc(DIMENSION_PREFIX + STRING_TYPE);

  // internal types
  public static ValueDesc MAP = new ValueDesc(MAP_TYPE);
  public static ValueDesc LIST = new ValueDesc(LIST_TYPE);
  public static ValueDesc INDEXED_ID = new ValueDesc(INDEXED_ID_TYPE);

  // from expression
  public static ValueDesc DECIMAL = new ValueDesc(DECIMAL_TYPE);
  public static ValueDesc STRUCT = new ValueDesc(STRUCT_TYPE);
  public static ValueDesc UNKNOWN = new ValueDesc(UNKNOWN_TYPE);

  public static ValueDesc STRING_ARRAY = new ValueDesc(ARRAY_PREFIX + STRING_TYPE);
  public static ValueDesc LONG_ARRAY = new ValueDesc(ARRAY_PREFIX + LONG_TYPE);
  public static ValueDesc FLOAT_ARRAY = new ValueDesc(ARRAY_PREFIX + FLOAT_TYPE);
  public static ValueDesc DOUBLE_ARRAY = new ValueDesc(ARRAY_PREFIX + DOUBLE_TYPE);

  public static ValueDesc ofArray(ValueDesc valueType)
  {
    return ofArray(valueType.typeName);
  }

  public static ValueDesc ofArray(String typeName)
  {
    return ValueDesc.of(ARRAY_PREFIX + typeName);
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

  public static boolean isDecimal(ValueDesc valueType)
  {
    return valueType != null && valueType.typeName.startsWith(DECIMAL_TYPE);
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

  public static ValueDesc toCommonType(@Nullable ValueDesc type1, ValueDesc type2)
  {
    if (type1 == null) {
      return type2;
    }
    if (type1.equals(type2)) {
      return type1;
    }
    if (type1.isNumeric() && type2.isNumeric()) {
      return ValueDesc.DOUBLE;
    }
    if (type1.isStringOrDimension() && type2.isStringOrDimension()) {
      return ValueDesc.STRING;
    }
    return ValueDesc.UNKNOWN;
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

  private ValueDesc(ValueType primitive)
  {
    Preconditions.checkArgument(primitive.isPrimitive(), "should be primitive type");
    this.type = primitive;
    this.typeName = primitive.getName();
  }

  private ValueDesc(String typeName)
  {
    this.type = ValueType.of(Preconditions.checkNotNull(typeName, "typeName cannot be null"));
    this.typeName = type.isPrimitive() ? type.getName() : normalize(typeName);
  }

  // complex types are case sensitive (same with serde-name) but primitive types are not.. fuck
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
    return typeName == null ? null : new ValueDesc(typeName);
  }

  public static ValueDesc of(ValueType valueType)
  {
    return valueType == null ? ValueDesc.UNKNOWN : new ValueDesc(valueType);
  }

  public static boolean isMap(ValueDesc type)
  {
    return type != null && type.typeName.equals(MAP_TYPE);
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

  public static boolean isNumeric(ValueDesc type)
  {
    return type != null && type.isNumeric();
  }

  public static boolean isUnknown(ValueDesc type)
  {
    return isType(type, UNKNOWN_TYPE);
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

  public boolean isNumeric()
  {
    return type.isNumeric();
  }

  public boolean isString()
  {
    return type == ValueType.STRING;
  }

  public boolean isDimension()
  {
    return isDimension(typeName);
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
    return typeName.toLowerCase().startsWith(STRUCT_TYPE);
  }

  public Comparable cast(Object value)
  {
    return type.cast(value);
  }
}
