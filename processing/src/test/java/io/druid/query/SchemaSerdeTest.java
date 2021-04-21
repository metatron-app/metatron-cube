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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.druid.data.ValueDesc;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class SchemaSerdeTest
{
  private static final ObjectMapper MAPPER = TestHelper.JSON_MAPPER;
  private static final TypeFactory FACTORY = MAPPER.getTypeFactory();
  private static final JavaType T1 = FACTORY.constructType(new TypeReference<Schema>() {});
  private static final JavaType T2 = FACTORY.constructParametricType(Result.class, BySegmentSchemaValue.class);

  private static final Schema SCHEMA = new Schema(
      Arrays.asList("xx", "yy"),
      Arrays.asList(ValueDesc.DIM_STRING, ValueDesc.DIM_STRING)
  );

  @Test
  public void test() throws IOException
  {
    String content = MAPPER.writeValueAsString(SCHEMA);
    Assert.assertEquals(
        "{\"type\":\"schema\",\"columnNames\":[\"xx\",\"yy\"],\"columnTypes\":[\"dimension.string\",\"dimension.string\"]}", content
    );
    Schema read = MAPPER.<Schema>readValue(content, T1);
    Assert.assertEquals(SCHEMA, read);

    Result<BySegmentResultValue<Schema>> r = new Result<BySegmentResultValue<Schema>>(
        new DateTime(1000), new BySegmentSchemaValue(SCHEMA, "222", null)
    );
    content = MAPPER.writeValueAsString(r);
    Assert.assertEquals(
        "{\"timestamp\":\"1970-01-01T00:00:01.000Z\",\"result\":{\"results\":{\"type\":\"schema\",\"columnNames\":[\"xx\",\"yy\"],\"columnTypes\":[\"dimension.string\",\"dimension.string\"]},\"segment\":\"222\",\"interval\":null}}",
        content
    );
    Result<BySegmentResultValue<Schema>> r1 = MAPPER.readValue(content, T2);
    read = r1.getValue().getResults().get(0);
    Assert.assertEquals(SCHEMA, read);
  }
}
