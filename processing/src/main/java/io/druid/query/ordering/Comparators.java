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

import com.google.common.collect.Ordering;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;

import java.util.Comparator;

/**
 */
public class Comparators
{
  public static Comparator createGeneric(String name, Comparator defaultValue)
  {
    if (StringUtils.isNullOrEmpty(name)) {
      return defaultValue;
    }
    boolean descending = false;
    String lowerCased = name.toLowerCase();
    if (lowerCased.endsWith(":asc")) {
      name = name.substring(0, name.length() - 4);
    } else if (lowerCased.endsWith(":desc")) {
      name = name.substring(0, name.length() - 5);
      descending = true;
    }
    Comparator comparator = createString(name, defaultValue);
    return descending ? Ordering.from(defaultValue).reverse() : comparator;
  }

  private static Comparator createString(String name, Comparator defaultValue)
  {
    if (StringUtils.isNullOrEmpty(name)) {
      return defaultValue;
    }
    ValueDesc type = ValueDesc.of(name);
    if (type.isPrimitive()) {
      return type.comparator();
    }
    Comparator comparator = StringComparators.tryMakeComparator(name, null);
    if (comparator == null) {
      return defaultValue;
    }
    return comparator;
  }
}
