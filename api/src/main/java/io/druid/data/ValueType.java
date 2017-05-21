package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.data.input.Row;
import org.joda.time.DateTime;

import java.util.Comparator;

/**
 */
public enum ValueType
{
  FLOAT {
    @Override
    public Class classOfObject()
    {
      return Float.TYPE;
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
  },
  LONG {
    @Override
    public Class classOfObject()
    {
      return Long.TYPE;
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
  },
  DOUBLE {
    @Override
    public Class classOfObject()
    {
      return Double.TYPE;
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
  };

  public abstract Class classOfObject();

  public abstract Comparator comparator();

  public Object get(Row row, String column)
  {
    return row.getRaw(column);
  }

  @JsonValue
  @Override
  public String toString()
  {
    return this.name().toUpperCase();
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

  public static boolean isNumeric(ValueType type)
  {
    return type == DOUBLE || type == FLOAT || type == LONG;
  }
}
