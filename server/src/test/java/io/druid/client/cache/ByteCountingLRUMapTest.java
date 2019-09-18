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

package io.druid.client.cache;

import com.google.common.collect.Lists;
import io.druid.common.guava.ByteArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

/**
 */
public class ByteCountingLRUMapTest
{
  private ByteCountingLRUMap map;

  @Before
  public void setUp() throws Exception
  {
    map = new ByteCountingLRUMap(100);
  }

  @Test
  public void testSanity() throws Exception
  {
    final ByteArray tenKey = ByteArray.allocate(10);
    final byte[] eightyEightVal = ByteArray.allocate(88).array();
    final byte[] eightNineNineVal = ByteArray.allocate(89).array();
    final ByteArray oneByte = ByteArray.allocate(1);
    final ByteArray twoByte = ByteArray.allocate(2);

    assertMapValues(0, 0, 0);
    map.put(tenKey, eightNineNineVal);
    assertMapValues(1, 99, 0);
    Assert.assertEquals(ByteArray.wrap(eightNineNineVal), ByteArray.wrap(map.get(tenKey)));

    map.put(oneByte, oneByte.array());
    assertMapValues(1, 2, 1);
    Assert.assertNull(map.get(tenKey));
    Assert.assertEquals(oneByte, ByteArray.wrap(map.get(oneByte)));

    map.put(tenKey, eightyEightVal);
    assertMapValues(2, 100, 1);
    Assert.assertEquals(oneByte, ByteArray.wrap(map.get(oneByte)));
    Assert.assertEquals(ByteArray.wrap(eightyEightVal), ByteArray.wrap(map.get(tenKey)));

    map.put(twoByte, oneByte.array());
    assertMapValues(2, 101, 2);
    Assert.assertEquals(ByteArray.wrap(eightyEightVal), ByteArray.wrap(map.get(tenKey)));
    Assert.assertEquals(oneByte, ByteArray.wrap(map.get(twoByte)));

    Iterator<ByteArray> it = map.keySet().iterator();
    List<ByteArray> toRemove = Lists.newLinkedList();
    while(it.hasNext()) {
      ByteArray buf = it.next();
      if(buf.length() == 10) {
        toRemove.add(buf);
      }
    }
    for(ByteArray buf : toRemove) {
      map.remove(buf);
    }
    assertMapValues(1, 3, 2);

    map.remove(twoByte);
    assertMapValues(0, 0, 2);
  }

  private void assertMapValues(final int size, final int numBytes, final int evictionCount)
  {
    Assert.assertEquals(size, map.size());
    Assert.assertEquals(numBytes, map.getNumBytes());
    Assert.assertEquals(evictionCount, map.getEvictionCount());
  }
}
