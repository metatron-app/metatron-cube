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

package io.druid.collections;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class String2IntMapTest
{
  private String2IntMap mapObject;

  @Before
  public void setUp()
  {
    mapObject = new String2IntMap();
  }

  @After
  public void tearDown()
  {
    mapObject.clear();
  }

  @Test
  public void testAdd()
  {
    String defaultKey1 = "defaultKey1";
    String defaultKey2 = "defaultKey2";
    String defaultKey3 = "defaultKey3";
    mapObject.addTo(defaultKey1, 10);
    mapObject.addTo(defaultKey2, 20);
    mapObject.addTo(defaultKey1, 5);
    Assert.assertEquals("Values does not match", 15, mapObject.getInt(defaultKey1));
    Assert.assertEquals("Values does not match", 20, mapObject.getInt(defaultKey2));
    Assert.assertEquals("Values does not match", 0, mapObject.getInt(defaultKey3));
  }
}
