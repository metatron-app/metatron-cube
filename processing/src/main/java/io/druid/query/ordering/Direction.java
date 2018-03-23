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

package io.druid.query.ordering;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.druid.common.utils.StringUtils;

public enum Direction
{
  ASCENDING,
  DESCENDING;

  @Override
  @JsonValue
  public String toString()
  {
    return name().toLowerCase();
  }

  @JsonCreator
  public static Direction fromString(String name)
  {
    Direction direction = tryFromString(name);
    if (direction == null) {
      throw new IllegalArgumentException("invalid direction " + name);
    }
    return direction;
  }

  public static Direction tryFromString(String name)
  {
    if (StringUtils.isNullOrEmpty(name)) {
      return ASCENDING;
    }
    name = name.toUpperCase();
    if (name.equals("DESC") || name.equals("DESCENDING")) {
      return DESCENDING;
    } else if (name.equals("ASC") || name.equals("ASCENDING")) {
      return ASCENDING;
    }
    return null;
  }
}

