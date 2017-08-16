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

/**
 */
public class ValueDesc
{
  // primitives (should be conform with ValueType.name())
  public static final String STRING_TYPE = "STRING";
  public static final String FLOAT_TYPE = "FLOAT";
  public static final String DOUBLE_TYPE = "DOUBLE";
  public static final String LONG_TYPE = "LONG";

  // non primitives
  public static final String MAP_TYPE = "map";
  public static final String LIST_TYPE = "list";
  public static final String INDEXED_ID_TYPE = "indexed";
  public static final String DATETIME_TYPE = "datetime";
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

  // primitives
  public static ValueDesc STRING = new ValueDesc(ValueType.STRING, STRING_TYPE);
  public static ValueDesc FLOAT = new ValueDesc(ValueType.FLOAT, FLOAT_TYPE);
  public static ValueDesc DOUBLE = new ValueDesc(ValueType.DOUBLE, DOUBLE_TYPE);
  public static ValueDesc LONG = new ValueDesc(ValueType.LONG, LONG_TYPE);

  // internal types
  public static ValueDesc MAP = of(MAP_TYPE);
  public static ValueDesc LIST = of(LIST_TYPE);
  public static ValueDesc INDEXED_ID = of(INDEXED_ID_TYPE);

  // from expression
  public static ValueDesc DATETIME = of(DATETIME_TYPE);
  public static ValueDesc UNKNOWN = of(UNKNOWN_TYPE);

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
    return of(DIMENSION_PREFIX + valueType.name());
  }

  public static ValueDesc ofMultiValued(ValueType valueType)
  {
    return ValueDesc.of(MULTIVALUED_PREFIX + valueType.name());
  }

  public static ValueDesc ofIndexedId(ValueType valueType)
  {
    return ValueDesc.of(INDEXED_ID_PREFIX + valueType.name());
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
    return isDimension(valueType.typeName);
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

  public static String subElementOf(ValueDesc valueType)
  {
    return valueType == null ? null : subElementOf(valueType.typeName);
  }

  public static String subElementOf(String typeName)
  {
    int index = typeName.indexOf('.');
    return index < 0 ? null : typeName.substring(index + 1);
  }

  private final ValueType type;
  private final String typeName;

  private ValueDesc(ValueType type, String typeName)
  {
    this.type = type;
    this.typeName = typeName;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ValueDesc valueType = (ValueDesc) o;

    if (!typeName.equals(valueType.typeName)) {
      return false;
    }

    return true;
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
    return typeName == null ? ValueDesc.UNKNOWN : new ValueDesc(ValueType.of(typeName), typeName);
  }

  public static ValueDesc of(ValueType valueType)
  {
    return valueType == null ? ValueDesc.UNKNOWN : new ValueDesc(valueType, valueType.name().toLowerCase());
  }

  public static boolean isMap(ValueDesc type)
  {
    return type.typeName.equals(MAP_TYPE);
  }

  public static boolean isString(ValueDesc type)
  {
    return type.type == ValueType.STRING;
  }

  public static boolean isPrimitive(ValueDesc type)
  {
    return type.type.isPrimitive();
  }

  public static boolean isNumeric(ValueDesc type)
  {
    return type.type.isNumeric();
  }
}
