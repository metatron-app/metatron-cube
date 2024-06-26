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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class JSONPathFieldSpec
{
  private final JSONPathFieldType type;
  private final String name;
  private final String expr;

  @JsonCreator
  public JSONPathFieldSpec(
      @JsonProperty("type") JSONPathFieldType type,
      @JsonProperty("name") String name,
      @JsonProperty("expr") String expr
  )
  {
    this.type = type;
    this.name = name;
    this.expr = expr;
  }

  @JsonProperty
  public JSONPathFieldType getType()
  {
    return type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getExpr()
  {
    return expr;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, name, expr);
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

    JSONPathFieldSpec that = (JSONPathFieldSpec) o;

    if (!Objects.equals(expr, that.expr)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (type != that.type) {
      return false;
    }

    return true;
  }

  @JsonCreator
  public static JSONPathFieldSpec fromString(String name)
  {
    return JSONPathFieldSpec.createRootField(name);
  }

  public static JSONPathFieldSpec createNestedField(String name, String expr)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.PATH, name, expr);
  }

  public static JSONPathFieldSpec createRootField(String name)
  {
    return new JSONPathFieldSpec(JSONPathFieldType.ROOT, name, name);
  }

  public static List<JSONPathFieldSpec> createRootFields(String... names)
  {
    List<JSONPathFieldSpec> specs = Lists.newArrayList();
    for (String name : names) {
      specs.add(createRootField(name));
    }
    return specs;
  }
}
