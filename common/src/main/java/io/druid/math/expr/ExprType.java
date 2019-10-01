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

package io.druid.math.expr;

import com.google.common.base.Strings;
import io.druid.data.ValueDesc;

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
      case "BOOLEAN":
        return ValueDesc.BOOLEAN;
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
    }
    return ValueDesc.of(name);
  }

  public static String sqlType(ValueDesc desc)
  {
    if (desc == null || desc.isUnknown()) {
      return "string";
    }
    if (desc.isDimension()) {
      return ValueDesc.subElementOf(desc.typeName());
    }
    if (desc.isLong()) {
      return "bigint";
    }
    return desc.typeName().toLowerCase();
  }
}
