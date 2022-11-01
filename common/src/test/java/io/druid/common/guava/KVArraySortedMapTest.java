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

package io.druid.common.guava;

import org.junit.Assert;
import org.junit.Test;

public class KVArraySortedMapTest
{
  @Test
  public void test() {
    KVArraySortedMap<String, String> map = KVArraySortedMap.of("k1", "v1");
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("v1", map.get("k1"));
    Assert.assertNull(map.get("k2"));
    Assert.assertEquals("v1", map.put("k1", "v2"));
    Assert.assertEquals(1, map.size());

    Assert.assertNull(map.put("k0", "v0"));
    Assert.assertNull(map.put("k4", "v5"));
    Assert.assertNull(map.put("k2", "v3"));
    Assert.assertNull(map.put("k3", "v4"));
    Assert.assertEquals(5, map.size());
    Assert.assertEquals("v0", map.get("k0"));
    Assert.assertEquals("v2", map.get("k1"));
    Assert.assertEquals("v3", map.get("k2"));
    Assert.assertEquals("v4", map.get("k3"));
    Assert.assertEquals("v5", map.get("k4"));

    Assert.assertNull(map.remove("k21"));
    Assert.assertEquals(5, map.size());
    Assert.assertEquals("v3", map.remove("k2"));
    Assert.assertEquals(4, map.size());
    Assert.assertEquals("v5", map.remove("k4"));
    Assert.assertEquals(3, map.size());
    Assert.assertEquals("v0", map.remove("k0"));
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("v2", map.remove("k1"));
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("v4", map.remove("k3"));
    Assert.assertEquals(0, map.size());
  }
}