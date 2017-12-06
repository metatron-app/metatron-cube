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

package io.druid.hive;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class HivePathSpecTest
{
  @Test
  public void testSerDe() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    String x = "{\n" +
               "  \"type\" : \"hive\",\n" +
               "  \"source\" : \"default.lineitem\",\n" +
               "  \"metastoreUri\" : \"thrift://navisui-MacBook-Pro.local:9083\"\n" +
               "}";

    HivePathSpec y = mapper.readValue(x, HivePathSpec.class);
    System.out.println("[HivePathSpec/main] " + y);

    String z = mapper.writeValueAsString(y);
    System.out.println("[HivePathSpec/main] " + z);

    HivePathSpec w = mapper.readValue(z, HivePathSpec.class);
    System.out.println("[HivePathSpec/main] " + w);

    Assert.assertEquals(y, w);

    Map p = mapper.copy().setSerializationInclusion(JsonInclude.Include.NON_EMPTY).convertValue(y, Map.class);
    System.out.println("[HivePathSpec/main] " + p);
  }
}