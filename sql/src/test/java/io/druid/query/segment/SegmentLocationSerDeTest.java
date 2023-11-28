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

package io.druid.query.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.jackson.JacksonModule;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class SegmentLocationSerDeTest
{
  @Test
  public void test() throws Exception
  {
    ObjectMapper mapper = new JacksonModule().smileMapper();
    File source = new File(SegmentLocationSerDeTest.class.getClassLoader().getResource("b/0/00000.smoosh").getFile()).getParentFile();
    SegmentLocation location = new SegmentLocation(null, source);
    System.out.println(location.getLocation());
    Map<String, Long> sizes1 = Maps.newHashMap();
    for (File file : location.getLocation().listFiles()) {
      sizes1.put(file.getName(), file.length());
    }

    SegmentLocation moved = mapper.readValue(mapper.writeValueAsBytes(location), SegmentLocation.class);
    System.out.println(moved.getLocation());
    Map<String, Long> sizes2 = Maps.newHashMap();
    for (File file : moved.getLocation().listFiles()) {
      sizes2.put(file.getName(), file.length());
    }
    Assert.assertNotEquals(location.getLocation(), moved.getLocation());
    Assert.assertEquals(sizes1, sizes2);
  }
}
