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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class NestedParseSpecTest
{
  private final ObjectMapper jsonMapper = new TestObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    HashMap<String, Boolean> feature = new HashMap<String, Boolean>();
    feature.put("ALLOW_UNQUOTED_CONTROL_CHARS", true);
    JSONParseSpec rootSpec = new JSONParseSpec(
        new DefaultTimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo")), null, null),
        new JSONPathSpec(false, JSONPathFieldSpec.createRootFields("timestamp", "foo", "bar")),
        feature
    );
    JSONParseSpec columnSpec = new JSONParseSpec(
        null,
        null,
        new JSONPathSpec(false, JSONPathFieldSpec.createRootFields("x", "y")),
        feature
    );
    NestedParseSpec nestedSpec = new NestedParseSpec(
        rootSpec,
        ImmutableMap.<String, ParseSpec>of("bar", columnSpec)
    );

    final NestedParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(nestedSpec),
        NestedParseSpec.class
    );
    Assert.assertEquals("timestamp", ((DefaultTimestampSpec)serde.getTimestampSpec()).getTimestampColumn());
    Assert.assertEquals("iso", ((DefaultTimestampSpec)serde.getTimestampSpec()).getTimestampFormat());

    Assert.assertEquals(Arrays.asList("bar", "foo"), serde.getDimensionsSpec().getDimensionNames());
    JSONParseSpec root = (JSONParseSpec) serde.getRoot();
    Assert.assertEquals(rootSpec.getFlattenSpec(), root.getFlattenSpec());
    JSONParseSpec column = (JSONParseSpec) serde.getChildren().get("bar");
    Assert.assertEquals(columnSpec.getFlattenSpec(), column.getFlattenSpec());

    String x = "{ "
               + "\"timestamp\": 10000000, \"foo\": \"val1\", "
               + "\"bar\": \"{\\\"x\\\": \\\"val2\\\", \\\"y\\\": \\\"val3\\\"}\""
               + "}";
    Assert.assertEquals(
        ImmutableMap.<String, Object>of("timestamp", 10000000L, "foo", "val1", "x", "val2", "y", "val3"),
        nestedSpec.makeParser().parseToMap(x)
    );
  }
}
