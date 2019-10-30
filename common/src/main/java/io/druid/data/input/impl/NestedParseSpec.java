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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.parsers.Parser;
import io.druid.data.input.TimestampSpec;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class NestedParseSpec implements ParseSpec
{
  private final ParseSpec root;
  private final Map<String, ParseSpec> children;

  @JsonCreator
  public NestedParseSpec(
      @JsonProperty("root") ParseSpec root,
      @JsonProperty("children") Map<String, ParseSpec> children
  )
  {
    this.root = Preconditions.checkNotNull(root, "'root' should not be null");
    this.children = children;
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return root.getTimestampSpec();
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return root.getDimensionsSpec();
  }

  @JsonProperty
  public ParseSpec getRoot()
  {
    return root;
  }

  @JsonProperty
  public Map<String, ParseSpec> getChildren()
  {
    return children;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    final Parser<String, Object> parser = root.makeParser();
    if (children == null || children.isEmpty()) {
      return parser;
    }
    final Map<String, Parser<String, Object>> columnParsers = Maps.newHashMap();
    for (Map.Entry<String, ParseSpec> entry : children.entrySet()) {
      columnParsers.put(entry.getKey(), entry.getValue().makeParser());
    }
    return new AbstractParser<String, Object>()
    {
      @Override
      public Map<String, Object> parseToMap(String input)
      {
        final Map<String, Object> parsed = parser.parseToMap(input);
        final List<Map<String, Object>> columnParsedList = Lists.newArrayList();
        final Iterator<Map.Entry<String, Object>> iter = parsed.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, Object> entry = iter.next();
          Parser<String, Object> parser = columnParsers.get(entry.getKey());
          if (parser == null) {
            continue;
          }
          columnParsedList.add(parser.parseToMap(Objects.toString(entry.getValue(), null)));
          iter.remove();
        }
        for (Map<String, Object> columnParsed : columnParsedList) {
          parsed.putAll(columnParsed);
        }
        return parsed;
      }
    };
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new NestedParseSpec(root.withTimestampSpec(spec), children);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new NestedParseSpec(root.withDimensionsSpec(spec), children);
  }
}
