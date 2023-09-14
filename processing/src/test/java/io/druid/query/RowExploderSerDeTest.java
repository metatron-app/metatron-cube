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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.segment.StringArray;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RowExploderSerDeTest
{
  private static final ObjectMapper MAPPER = TestHelper.JSON_MAPPER;

  @Test
  public void testSerde() throws IOException
  {
    String column = "abc";
    Map<String, Integer> exploder = ImmutableMap.of("aaa", 1, "bbb", 2);
    List<String> outputColumns = Arrays.asList("xxx");
    RowExploder processor = RowExploder.of(column, exploder, outputColumns);
    String string = MAPPER.writeValueAsString(processor);

    RowExploder op = (RowExploder) MAPPER.readValue(string, PostProcessingOperator.class);
    Assert.assertEquals(Arrays.asList(column), op.getKeys());
    Assert.assertEquals(exploder, op.getExploder());
    Assert.assertEquals(outputColumns, op.getOutputColumns());
  }

  @Test
  public void testSerdeM() throws IOException
  {
    List<String> columns = Arrays.asList("abc", "def");
    Map<StringArray, Integer> exploderM = ImmutableMap.of(
        StringArray.of(new String[] {"aaa", "zzz"}), 1, StringArray.of(new String[] {"bbb", "kkk"}), 2
    );
    List<String> outputColumns = Arrays.asList("xxx");
    RowExploder processor = RowExploder.of(columns, new StringArray.ToIntMap(exploderM), outputColumns);
    String string = MAPPER.writeValueAsString(processor);

    RowExploder op = (RowExploder) MAPPER.readValue(string, PostProcessingOperator.class);
    Assert.assertEquals(columns, op.getKeys());
    Assert.assertEquals(exploderM, op.getExploderM());
    Assert.assertEquals(outputColumns, op.getOutputColumns());
  }
}
