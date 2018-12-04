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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 */
public class TypeUtils
{
  public static String[] splitDescriptiveType(String string)
  {
    Preconditions.checkNotNull(string);
    string = string.trim();
    int index1 = string.indexOf('(');
    if (index1 < 0 || !string.endsWith(")")) {
      return null;
    }
    final String type = string.substring(0, index1);
    final String description = string.substring(index1 + 1, string.length() - 1);
    final List<String> elements = TypeUtils.splitWithEscape(description, ',');
    elements.add(0, type);

    return elements.toArray(new String[0]);
  }

  public static List<String> splitWithEscape(String string, char target)
  {
    List<String> splits = Lists.newArrayList();
    int prev = 0;
    for (int i = seekWithEscape(string, prev, target); i >= 0; i = seekWithEscape(string, prev, target)) {
      splits.add(trimStartingSpaces(string.substring(prev, i)));
      prev = i + 1;
    }
    if (prev < string.length()) {
      splits.add(trimStartingSpaces(string.substring(prev)));
    }
    return splits;
  }

  public static int seekWithEscape(String string, int index, char target)
  {
    boolean escape = false;
    for (; index < string.length(); index++) {
      char c = string.charAt(index);
      if (c == '\'') {
        escape = !escape;
      }
      if (escape) {
        continue;
      }
      if (c == target) {
        return index;
      }
    }
    return -1;
  }

  private static String trimStartingSpaces(String string)
  {
    int i = 0;
    for (; i < string.length(); i++) {
      if (string.charAt(i) != ' ') {
        break;
      }
    }
    return string.substring(i);
  }
}
