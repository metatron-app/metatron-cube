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

package io.druid.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TypeUtils
{
  public static String parseType(JsonNode node)
  {
    try {
      if (node != null && node.isObject()) {
        return append(node, new StringBuilder()).toString();
      }
    }
    catch (Exception e) {
      // ignore
    }
    return null;
  }

  private static StringBuilder append(JsonNode node, StringBuilder b)
  {
    if (node.isObject()) {
      b.append("struct(");
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        b.append(entry.getKey()).append(':');
        append(entry.getValue(), b);
        if (fields.hasNext()) {
          b.append(',');
        }
      }
      return b.append(')');
    }
    return b.append(node.asText());
  }

  public static String[] splitDescriptiveType(ValueDesc type)
  {
    return splitDescriptiveType(type.typeName());
  }

  // struct(a:b,c:d) --> struct, a:b, c:d
  // struct(a:b,family:struct(c:d,e:f)) --> struct a:b family:struct(c:d,e:f)
  public static String[] splitDescriptiveType(String string)
  {
    if (string == null) {
      return null;
    }
    string = string.trim();
    int index1 = string.indexOf('(');
    if (index1 < 0 || !string.endsWith(")")) {
      return null;
    }
    final String type = string.substring(0, index1);
    final String description = string.substring(index1 + 1, string.length() - 1);
    // a:b,family:struct(c:d,e:f)
    final List<String> elements = TypeUtils.splitWithEscape(description, ',');
    elements.add(0, type);

    return elements.toArray(new String[0]);
  }

  public static List<String> splitWithEscape(String string, char target)
  {
    List<String> splits = Lists.newArrayList();
    int prev = 0;
    for (int i = seekWithEscape(string, prev, target); i >= 0; i = seekWithEscape(string, prev, target)) {
      int lx = seekWithEscape(string, prev, '(');
      if (lx > 0 && lx < i) {
        int rx = seekWithEscape(string, lx + 1, ')');
        for (lx = seekWithEscape(string, lx + 1, '('); lx > 0 && lx < rx;
             lx = seekWithEscape(string, lx + 1, '('), rx = seekWithEscape(string, rx + 1, ')')) {
        }
        if (rx > 0) {
          i = rx + 1;
        }
      }
      if (prev < i) {
        splits.add(trimStartingSpaces(string.substring(prev, i)));
      }
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
