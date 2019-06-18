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

package io.druid.query.aggregation.corr;

import io.druid.segment.data.ObjectStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

public class PearsonSerdeTest
{
  @Test
  public void testSerde()
  {
    Random r = new Random();
    PearsonAggregatorCollector holder = new PearsonAggregatorCollector();
    ObjectStrategy strategy = new PearsonSerde().getObjectStrategy();
    Assert.assertEquals(PearsonAggregatorCollector.class, strategy.getClazz());

    for (int i = 0; i < 100; i++) {
      byte[] array = strategy.toBytes(holder);
      Assert.assertArrayEquals(array, holder.toByteArray());
      Assert.assertEquals(holder, strategy.fromByteBuffer(ByteBuffer.wrap(array), array.length));
      holder.add(r.nextFloat(), r.nextFloat());
    }
  }
}
