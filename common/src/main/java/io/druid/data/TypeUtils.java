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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.common.IntTagged;
import io.druid.common.utils.StringUtils;

import java.util.Collections;
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
      if (node != null) {
        if (node.isObject()) {
          return append(node, new StringBuilder()).toString();
        } else if (node.isValueNode()) {
          return node.asText();
        }
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
    if (node.isArray()) {
      Preconditions.checkArgument(node.size() == 1);
      JsonNode element = Iterators.getOnlyElement(node.elements());
      if (element.isObject()) {
        b.append("array(");
        append(element, b);
        return b.append(')');
      }
      b.append("array.");
      return append(element, b);
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
    final List<String> elements = TypeUtils.splitWithQuote(description, ',');
    elements.add(0, type);

    return elements.toArray(new String[0]);
  }

  public static List<String> splitWithQuote(String string, char target)
  {
    List<String> splits = Lists.newArrayList();
    int prev = 0;
    for (int i = seekWithQuote(string, prev, target); i >= 0; i = seekWithQuote(string, prev, target)) {
      int lx = seekWithQuote(string, prev, '(');
      if (lx > 0 && lx < i) {
        int rx = seekWithQuote(string, lx + 1, ')');
        for (lx = seekWithQuote(string, lx + 1, '('); lx > 0 && lx < rx;
             lx = seekWithQuote(string, lx + 1, '('), rx = seekWithQuote(string, rx + 1, ')')) {
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

  public static int seekWithQuote(String string, int index, char target)
  {
    boolean quoted = false;
    for (; index < string.length(); index++) {
      char c = string.charAt(index);
      if (c == '\'') {
        quoted = !quoted;
      }
      if (quoted) {
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

  public static List<IntTagged<String>> parseEnum(ValueDesc type)
  {
    return parseEnum(Preconditions.checkNotNull(TypeUtils.splitDescriptiveType(type)));
  }

  public static List<IntTagged<String>> parseEnum(String[] description)
  {
    List<IntTagged<String>> values = Lists.newArrayList();
    values.add(IntTagged.of(0, ""));
    for (int i = 1; i < description.length; i++) {
      values.add(IntTagged.of(i, StringUtils.unquote(description[i].trim())));
    }
    Collections.sort(values, (e1, e2) -> e1.value.compareTo(e2.value));
    return values;
  }
}
