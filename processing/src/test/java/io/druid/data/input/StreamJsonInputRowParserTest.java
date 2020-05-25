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

package io.druid.data.input;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StreamJsonInputRowParserTest
{
  @Test
  public void test() throws Exception
  {
    List<String> dimensions = Arrays.asList("deviceType", "emailId");
    Map<String, Object> row1 = ImmutableMap.of(
        "deviceType", "web", "emailId", "xyz@gmail.com", "timeStamp", "2020-04-28 00:01:12", "totalTime", 60
    );
    Map<String, Object> row2 = ImmutableMap.of(
        "deviceType", "mobile", "emailId", "abc@naver.com", "timeStamp", "2020-04-28 00:02:12", "totalTime", 20
    );
    ObjectWriter writer = new DefaultObjectMapper().writerWithDefaultPrettyPrinter();
    String multiLined = writer.writeValueAsString(row1) + "\n" + writer.writeValueAsString(row2);
    ByteArrayInputStream input1 = new ByteArrayInputStream(multiLined.getBytes());
    StringReader input2 = new StringReader(multiLined);

    TimestampSpec timeStamp = new DefaultTimestampSpec("timeStamp", "yyyy-MM-dd HH:mm:ss", null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.ofStringDimensions(dimensions);
    StreamJsonInputRowParser parser = new StreamJsonInputRowParser(timeStamp, dimensionsSpec, false);

    Assert.assertTrue(parser.accept(input1));
    List<InputRow> parsed = Lists.newArrayList(parser.parseStream(input1));
    Assert.assertEquals(2, parsed.size());
    Assert.assertEquals(new MapBasedInputRow(new DateTime("2020-04-28T00:01:12"), dimensions, row1), parsed.get(0));
    Assert.assertEquals(new MapBasedInputRow(new DateTime("2020-04-28T00:02:12"), dimensions, row2), parsed.get(1));

    Assert.assertTrue(parser.accept(input2));
    parsed = Lists.newArrayList(parser.parseStream(input2));
    Assert.assertEquals(2, parsed.size());
    Assert.assertEquals(new MapBasedInputRow(new DateTime("2020-04-28T00:01:12"), dimensions, row1), parsed.get(0));
    Assert.assertEquals(new MapBasedInputRow(new DateTime("2020-04-28T00:02:12"), dimensions, row2), parsed.get(1));
  }

  @Test
  public void testExplode() throws IOException
  {
    String json = new StringBuilder()
        .append("{\n")
        .append("  \"deviceType\": \"web\",\n")
        .append("  \"timeStamp\": \"2020-04-28 00:01:12\",\n")
        .append("  \"totalTime\": 60,\n")
        .append("  \"inactiveDuration\": 0,\n")
        .append("  \"emailId\": \"xyz@gmail.com\",\n")
        .append("  \"pageData\": [\n")
        .append("    {\n")
        .append("      \"pageDetails\": {\n")
        .append("        \"cohortId\": 123,\"pageType\": \"segmentView\",\"segmentId\": 456,\"time\": 60,\"moduleId\": 26340,\"url\": \"https://xyz.com\"\n")
        .append("      },\n")
        .append("      \"items\": [\n")
        .append("        {\n")
        .append("          \"metadata\": {\n")
        .append("            \"currentTime\": 81.559263,\"lastPlaybackRate\": 1,\"isLocked\": false,\n")
        .append("            \"video\": {\n")
        .append("              \"videoType\": \"YOUTUBE\",\"viewingTime\": 403.3,\"videoSize\": 18434910,\"url\": \"6860001\"\n")
        .append("            }\n")
        .append("          },\n")
        .append("          \"subType\": \"video\",\"id\": 100001,\"time\": 60,\"type\": \"component\"\n")
        .append("        },\n")
        .append("        {\n")
        .append("          \"metadata\": {\n")
        .append("            \"questionCount\": 1,\"quizId\": 123,\"quizType\": \"internal\",\"title\": \"Quiz 123\",\"status\": \"finished\"\n")
        .append("          },\n")
        .append("          \"subType\": \"text\",\"id\": 13458,\"time\": 60,\"type\": \"component\"\n")
        .append("        }\n")
        .append("      ]\n")
        .append("    },\n")
        .append("    {\n")
        .append("      \"pageDetails\": {\n")
        .append("        \"cohortId\": 124,\"pageType\": \"segmentView\",\"segmentId\": 456,\"time\": 60,\"moduleId\": 26340,\"url\": \"https://xyz.com\"\n")
        .append("      },\n")
        .append("      \"items\": [\n")
        .append("        {\n")
        .append("          \"metadata\": {\n")
        .append("            \"currentTime\": 81.559263,\"lastPlaybackRate\": 1,\"isLocked\": false,\n")
        .append("            \"video\": {\n")
        .append("              \"videoType\": \"YOUTUBE\",\"viewingTime\": 403.3,\"videoSize\": 18434910,\"url\": \"6860001\"\n")
        .append("            }\n")
        .append("          },\n")
        .append("          \"subType\": \"video\",\"id\": 100002,\"time\": 60,\"type\": \"component\"\n")
        .append("        },\n")
        .append("        {\n")
        .append("          \"metadata\": {\n")
        .append("            \"questionCount\": 1,\"quizId\": 123,\"quizType\": \"internal\",\"title\": \"Quiz 123\",\"status\": \"finished\"\n")
        .append("          },\n")
        .append("          \"subType\": \"text\",\"id\": 13459,\"time\": 60,\"type\": \"component\"\n")
        .append("        }\n")
        .append("      ]\n")
        .append("    }\n")
        .append("  ],\n")
        .append("  \"userId\": 12344790,\n")
        .append("  \"platform\": \"web\"\n")
        .append("}")
        .toString();

    List<String> dimensions = Arrays.asList("deviceType", "emailId");
    TimestampSpec timeStamp = new DefaultTimestampSpec("timeStamp", "yyyy-MM-dd HH:mm:ss", null);
    DimensionsSpec dimensionsSpec = DimensionsSpec.ofStringDimensions(dimensions);
    StreamJsonExplodinInputRowParser parser = new StreamJsonExplodinInputRowParser(timeStamp, dimensionsSpec, false);
    Assert.assertEquals(8, Lists.newArrayList(parser.parseStream(new StringReader(json))).size());
  }
}
