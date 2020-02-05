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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.ExpressionTimestampSpec;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class CSVParseSpecTest
{
  @Test(expected = IllegalArgumentException.class)
  @Ignore("evaluation can make new columns")
  public void testColumnMissing() throws Exception
  {
    final ParseSpec spec = new CSVParseSpec(
        new DefaultTimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        Arrays.asList("a")
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComma() throws Exception
  {
    final ParseSpec spec = new CSVParseSpec(
        new DefaultTimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("a,", "b")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        ),
        ",",
        Arrays.asList("a")
    );
  }

  @Test
  public void testNullString() throws Exception
  {
    final ParseSpec spec = new CSVParseSpec(
        new ExpressionTimestampSpec("datetime('2020-01-08')"),
        DimensionsSpec.ofStringDimensions(Arrays.asList("a")),
        ",",
        Arrays.asList("a", "b"),
        "N/A",
        true
    );
    Map<String, Object> expected = Maps.newHashMap();
    expected.put("a", "value");
    expected.put("b", null);
    Assert.assertEquals(expected, spec.makeParser().parseToMap("value , N/A"));
  }
}
