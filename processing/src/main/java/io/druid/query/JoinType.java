/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 */
public enum JoinType
{
  INNER { public boolean isLeftDrivable()  { return true;} public boolean isRightDrivable() { return true;}},
  LO    { public boolean isLeftDrivable()  { return true;} public JoinType revert() { return RO; }},
  RO    { public boolean isRightDrivable() { return true;} public JoinType revert() { return LO; }},
  FULL  { public JoinType revert() { throw new UnsupportedOperationException("revert"); }};

  public boolean isLeftDrivable()
  {
    return false;
  }

  public boolean isRightDrivable()
  {
    return false;
  }

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static JoinType fromString(String name)
  {
    if (name == null) {
      return null;
    }
    name = name.toUpperCase();
    try {
      return valueOf(name);
    }
    catch (IllegalArgumentException e) {
      if (name.contains("LEFT")) {
        return LO;
      } else if (name.contains("RIGHT")) {
        return RO;
      }
      throw e;
    }
  }

  public JoinType revert()
  {
    return this;
  }
}
