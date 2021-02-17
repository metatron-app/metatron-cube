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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.common.guava.GuavaUtils;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Objects;

/**
 */
public enum ValueType
{
  BOOLEAN {
    @Override
    public String getName()
    {
      return ValueDesc.BOOLEAN_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return Boolean.class;
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseBoolean(value);
    }

    @Override
    public Comparable castIfPossible(Object value)
    {
      return Rows.parseBooleanIfPossible(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Byte.SIZE;
    }

    @Override
    public boolean isNumeric()
    {
      return false;
    }
  },
  FLOAT {
    @Override
    public String getName()
    {
      return ValueDesc.FLOAT_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return Float.class;
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseFloat(value);
    }

    @Override
    public Comparable castIfPossible(Object value)
    {
      return Rows.parseFloatIfPossible(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Float.SIZE;
    }
  },
  LONG {
    @Override
    public String getName()
    {
      return ValueDesc.LONG_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return Long.class;
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseLong(value);
    }

    @Override
    public Comparable castIfPossible(Object value)
    {
      return Rows.parseLongIfPossible(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Long.SIZE;
    }
  },
  DOUBLE {
    @Override
    public String getName()
    {
      return ValueDesc.DOUBLE_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return Double.class;
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseDouble(value);
    }

    @Override
    public Comparable castIfPossible(Object value)
    {
      return Rows.parseDoubleIfPossible(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Double.SIZE;
    }
  },
  STRING {
    @Override
    public String getName()
    {
      return ValueDesc.STRING_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return String.class;
    }

    @Override
    public boolean isNumeric()
    {
      return false;
    }

    @Override
    public Comparable cast(Object value)
    {
      return Objects.toString(value, null);
    }
  },
  DATETIME {
    @Override
    public String getName()
    {
      return ValueDesc.DATETIME_TYPE;
    }

    @Override
    public Class classOfObject()
    {
      return DateTime.class;
    }

    @Override
    public boolean isNumeric()
    {
      return false;
    }

    @Override
    public Object cast(Object value)
    {
      if (value instanceof DateTime) {
        return value;
      } else if (value instanceof Number) {
        return new DateTime(((Number) value).longValue(), ISOChronology.getInstanceUTC());
      }
      return super.cast(value);
    }
  },
  COMPLEX {
    @Override
    public Class classOfObject()
    {
      return Object.class;
    }

    @Override
    public Comparator comparator()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNumeric()
    {
      return false;
    }

    @Override
    public boolean isPrimitive()
    {
      return false;
    }
  };

  public abstract Class classOfObject();

  public Comparator comparator()
  {
    return GuavaUtils.NULL_FIRST_NATURAL;
  }

  public Object cast(Object value)
  {
    return classOfObject().cast(value);
  }

  public Object castIfPossible(Object value)
  {
    try {
      return cast(value);
    }
    catch (Exception e) {
      return value instanceof Comparable ? (Comparable) value : null;
    }
  }

  public boolean isNumeric()
  {
    return true;
  }

  public int lengthOfBinary()
  {
    return -1;
  }

  public boolean isPrimitive()
  {
    return true;
  }

  @JsonValue
  public String getName()
  {
    return name().toLowerCase();
  }

  @Nullable
  @JsonCreator
  public static ValueType fromString(@Nullable String name)
  {
    if (Strings.isNullOrEmpty(name)) {
      return null;
    }
    switch (name.toUpperCase()) {
      case "BOOLEAN" : return BOOLEAN;
      case "FLOAT" : return FLOAT;
      case "DOUBLE" : return DOUBLE;
      case "LONG" : return LONG;
      case "DATETIME" : return DATETIME;
      case "STRING" : return STRING;
      case "COMPLEX" : return COMPLEX;
    }
    return null;
  }

  public static ValueType of(String name)
  {
    return of(name, COMPLEX);
  }

  public static ValueType of(String name, ValueType defaultType)
  {
    return Optional.fromNullable(fromString(name)).or(defaultType);
  }

  public static ValueType ofPrimitive(String name)
  {
    ValueType type = of(name);
    Preconditions.checkArgument(type.isPrimitive(), name + " is not primitive type");
    return type;
  }

  public static ValueType of(Class clazz)
  {
    if (clazz == String.class) {
      return STRING;
    }
    if (clazz == Boolean.TYPE || clazz == Boolean.class) {
      return BOOLEAN;
    }
    if (clazz == Byte.TYPE || clazz == Byte.class ||
        clazz == Short.TYPE || clazz == Short.class ||
        clazz == Integer.TYPE || clazz == Integer.class ||
        clazz == Long.TYPE || clazz == Long.class) {
      return LONG;
    }
    if (clazz == Float.TYPE || clazz == Float.class) {
      return FLOAT;
    }
    if (clazz == Double.TYPE || clazz == Double.class) {
      return DOUBLE;
    }
    if (clazz == DateTime.class) {
      return DATETIME;
    }
    if (clazz == Timestamp.class) {
      return LONG;
    }
    return COMPLEX;
  }
}
