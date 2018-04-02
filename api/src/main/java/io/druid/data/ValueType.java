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
import com.google.common.collect.Ordering;
import io.druid.data.input.Row;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.Comparator;
import java.util.Objects;

/**
 */
public enum ValueType
{
  FLOAT {
    @Override
    public Class classOfObject()
    {
      return Float.class;
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getFloatMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseFloat(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Float.SIZE;
    }
  },
  LONG {
    @Override
    public Class classOfObject()
    {
      return Long.class;
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getLongMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseLong(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Long.SIZE;
    }
  },
  DOUBLE {
    @Override
    public Class classOfObject()
    {
      return Double.class;
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getDoubleMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      return Rows.parseDouble(value);
    }

    @Override
    public int lengthOfBinary()
    {
      return Double.SIZE;
    }
  },
  STRING {
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
    public Object get(Row row, String column)
    {
      return Objects.toString(row.getRaw(column), null);
    }

    @Override
    public Comparable cast(Object value)
    {
      return Objects.toString(value, null);
    }
  },
  DATETIME {
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
    public Object get(Row row, String column)
    {
      Object value = row.getRaw(column);
      if (value instanceof DateTime) {
        return value;
      } else if (value instanceof Number) {
        return new DateTime(((Number)value).longValue(), ISOChronology.getInstanceUTC());
      }
      return value;
    }

    @Override
    public Comparable cast(Object value)
    {
      if (value instanceof DateTime) {
        return (DateTime) value;
      } else if (value instanceof Number) {
        return new DateTime(((Number)value).longValue(), ISOChronology.getInstanceUTC());
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
    return Ordering.natural().nullsFirst();
  }

  public Comparable cast(Object value)
  {
    return (Comparable) classOfObject().cast(value);
  }

  public Object get(Row row, String column)
  {
    return row.getRaw(column);
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

  @JsonCreator
  public static ValueType fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }

  public static ValueType of(String name)
  {
    return of(name, COMPLEX);
  }

  public static ValueType of(String name, ValueType defaultType)
  {
    try {
      return name == null ? defaultType : fromString(name);
    }
    catch (IllegalArgumentException e) {
      return defaultType;
    }
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
    if (clazz == Long.TYPE || clazz == Long.class) {
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
    return COMPLEX;
  }
}
