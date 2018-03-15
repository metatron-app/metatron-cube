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

import java.math.BigDecimal;
import java.util.Map;

/**
 */
public class ExprType
{
  public static ValueDesc bestEffortOf(String name)
  {
    if (Strings.isNullOrEmpty(name)) {
      return ValueDesc.STRING;
    }
    switch (name.toUpperCase()) {
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
      case "DATETIME":
        return ValueDesc.DATETIME;
      case "STRING":
        return ValueDesc.STRING;
      default:
        return ValueDesc.of(name);
    }
  }

  public static ValueDesc typeOf(Class clazz)
  {
    if (clazz == String.class) {
      return ValueDesc.STRING;
    }
    if (clazz == Long.TYPE || clazz == Long.class) {
      return ValueDesc.LONG;
    }
    if (clazz == Float.TYPE || clazz == Float.class) {
      return ValueDesc.FLOAT;
    }
    if (clazz == Double.TYPE || clazz == Double.class) {
      return ValueDesc.DOUBLE;
    }
    if (clazz == DateTime.class) {
      return ValueDesc.DATETIME;
    }
    if (clazz == BigDecimal.class) {
      return ValueDesc.DECIMAL;
    }
    if (clazz == Map.class) {
      return ValueDesc.MAP;
    }
    return ValueDesc.UNKNOWN;
  }

  public static Class asClass(ValueDesc valueDesc)
  {
    if (valueDesc.isPrimitive()) {
      return valueDesc.type().classOfObject();
    }
    if (ValueDesc.isDecimal(valueDesc)) {
      return BigDecimal.class;
    }
    if (ValueDesc.isMap(valueDesc)) {
      return Map.class;
    }
    return Object.class;
  }
}
