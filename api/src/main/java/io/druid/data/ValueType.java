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
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;

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
    public Comparator<Float> comparator()
    {
      return new Comparator<Float>()
      {
        @Override
        public int compare(Float o1, Float o2)
        {
          return Floats.compare(o1, o2);
        }
      };
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getFloatMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      if (value instanceof Number) {
        return ((Number) value).floatValue();
      }
      return super.cast(value);
    }
  },
  LONG {
    @Override
    public Class classOfObject()
    {
      return Long.class;
    }

    @Override
    public Comparator<Long> comparator()
    {
      return new Comparator<Long>()
      {
        @Override
        public int compare(Long o1, Long o2)
        {
          return Longs.compare(o1, o2);
        }
      };
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getLongMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      return super.cast(value);
    }
  },
  DOUBLE {
    @Override
    public Class classOfObject()
    {
      return Double.class;
    }

    @Override
    public Comparator<Double> comparator()
    {
      return new Comparator<Double>()
      {
        @Override
        public int compare(Double o1, Double o2)
        {
          return Doubles.compare(o1, o2);
        }
      };
    }

    @Override
    public Object get(Row row, String column)
    {
      return row.getDoubleMetric(column);
    }

    @Override
    public Comparable cast(Object value)
    {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      return super.cast(value);
    }
  },
  STRING {
    @Override
    public Class classOfObject()
    {
      return String.class;
    }

    public Comparator<String> comparator()
    {
      return Ordering.natural().nullsFirst();
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

  public abstract Comparator comparator();

  public Comparable cast(Object value)
  {
    throw new UnsupportedOperationException("cast");
  }

  public Object get(Row row, String column)
  {
    return row.getRaw(column);
  }

  public boolean isNumeric()
  {
    return true;
  }

  public boolean isPrimitive()
  {
    return true;
  }

  @JsonValue
  @Override
  public String toString()
  {
    return this.name().toLowerCase();
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
    return COMPLEX;
  }
}
