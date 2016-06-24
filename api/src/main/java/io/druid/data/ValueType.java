package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

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
  },
  LONG {
    @Override
    public Class classOfObject()
    {
      return Long.TYPE;
    }
  },
  DOUBLE {
    @Override
    public Class classOfObject()
    {
      return Double.TYPE;
    }
  },
  STRING {
    @Override
    public Class classOfObject()
    {
      return String.class;
    }
  },
  COMPLEX {
    @Override
    public Class classOfObject()
    {
      return Object.class;
    }
  };

  public abstract Class classOfObject();

  @JsonValue
  @Override
  public String toString()
  {
    return this.name().toUpperCase();
  }

  @JsonCreator
  public static ValueType fromString(String name)
  {
    return valueOf(name.toUpperCase());
  }

  public static ValueType of(String name)
  {
    try {
      return name == null ? COMPLEX : fromString(name);
    }
    catch (IllegalArgumentException e) {
      return COMPLEX;
    }
  }

  public static boolean isNumeric(ValueType type)
  {
    return type == DOUBLE || type == FLOAT || type == LONG;
  }
}
