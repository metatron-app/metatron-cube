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

package io.druid.math.expr;

import com.google.common.base.Strings;
import io.druid.data.ValueDesc;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public enum ExprType
{
  DOUBLE {
    @Override
    public boolean isNumeric() { return true; }
    @Override
    public ValueDesc asValueDesc() { return ValueDesc.DOUBLE; }
  },
  LONG {
    @Override
    public boolean isNumeric() { return true; }
    @Override
    public ValueDesc asValueDesc() { return ValueDesc.LONG; }
  },
  DATETIME {
    @Override
    public boolean isNumeric() { return false; }
    @Override
    public ValueDesc asValueDesc() { return ValueDesc.DATETIME; }
  },
  STRING {
    @Override
    public boolean isNumeric() { return false; }
    @Override
    public ValueDesc asValueDesc() { return ValueDesc.STRING; }
  },
  UNKNOWN {
    @Override
    public boolean isNumeric() { return false; }
    @Override
    public ValueDesc asValueDesc() { return ValueDesc.UNKNOWN; }
  };

  public String typeName()
  {
    return name().toLowerCase();
  }

  public abstract boolean isNumeric();

  public abstract ValueDesc asValueDesc();

  public static ExprType bestEffortOf(String name)
  {
    if (Strings.isNullOrEmpty(name)) {
      return STRING;
    }
    switch (name.toUpperCase()) {
      case "FLOAT":
      case "DOUBLE":
        return DOUBLE;
      case "BYTE":
      case "SHORT":
      case "INT":
      case "INTEGER":
      case "LONG":
      case "BIGINT":
        return LONG;
      case "DATETIME":
        return DATETIME;
      case "STRING":
        return STRING;
      default:
        return UNKNOWN;
    }
  }

  public static ExprType typeOf(ValueDesc type)
  {
    switch (type.type()) {
      case LONG:
        return LONG;
      case FLOAT:
      case DOUBLE:
        return DOUBLE;
      case STRING:
        return STRING;
      case COMPLEX:
      default:
        return UNKNOWN;
    }
  }

  public static ExprType typeOf(Class clazz)
  {
    if (clazz == String.class) {
      return STRING;
    }
    if (clazz == Long.TYPE || clazz == Long.class) {
      return LONG;
    }
    if (clazz == Float.TYPE || clazz == Float.class || clazz == Double.TYPE || clazz == Double.class) {
      return DOUBLE;
    }
    if (clazz == DateTime.class) {
      return DATETIME;
    }
    return UNKNOWN;
  }

  public static abstract class StringFunction implements Function
  {
    @Override
    public final ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.STRING;
    }
  }

  public static abstract class LongFunction implements Function
  {
    @Override
    public final ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.LONG;
    }
  }

  public static abstract class DoubleFunction implements Function
  {
    @Override
    public final ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.DOUBLE;
    }
  }

  public static abstract class IndecisiveFunction implements Function
  {
    @Override
    public final ExprType apply(List<Expr> args, Expr.TypeBinding bindings)
    {
      return ExprType.UNKNOWN;
    }
  }
}
