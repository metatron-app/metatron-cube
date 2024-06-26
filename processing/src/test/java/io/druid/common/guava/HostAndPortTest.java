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

package io.druid.common.guava;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class HostAndPortTest
{
  @Test
  public void testSerDe() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    test(mapper, HostAndPort.fromParts("localhost", 8080), "\"localhost:8080\"");
    test(mapper, HostAndPort.fromString("localhost:8088"), "\"localhost:8088\"");
  }

  public void test(ObjectMapper mapper, HostAndPort hp, String string) throws java.io.IOException
  {
    Assert.assertEquals(string, mapper.writeValueAsString(hp));
    Assert.assertEquals(hp, mapper.readValue(string, HostAndPort.class));
  }
}
