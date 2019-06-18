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

package io.druid.query.aggregation.kurtosis;

import org.junit.Assert;
import org.junit.Test;


public class KurtosisAggregatorCollectorTest
{
  @Test
  public void testBasic()
  {
    KurtosisAggregatorCollector collector1 = new KurtosisAggregatorCollector();
    collector1.add(1);
    collector1.add(2);
    collector1.add(4);
    collector1.add(8);
    collector1.add(4);
    collector1.add(2);
    collector1.add(1);
    Assert.assertEquals(0.10843699296917331, collector1.getKurtosis(), 0.0001);

    KurtosisAggregatorCollector collector2 = new KurtosisAggregatorCollector();
    collector2.add(2);
    collector2.add(3);
    collector2.add(4);
    collector2.add(7);
    collector2.add(5);
    collector2.add(2);
    Assert.assertEquals(-0.8772809147153264, collector2.getKurtosis(), 0.0001);

    KurtosisAggregatorCollector collector3 = KurtosisAggregatorCollector.combineValues(collector1, collector2);
    Assert.assertEquals(-0.3065816857440171, collector3.getKurtosis(), 0.0001);

    KurtosisAggregatorCollector collector4 = KurtosisAggregatorCollector.combineValues(collector2, collector1);
    Assert.assertEquals(-0.4431567745441378, collector4.getKurtosis(), 0.0001);
  }

  @Test
  public void testConstant()
  {
    KurtosisAggregatorCollector collector1 = new KurtosisAggregatorCollector();
    collector1.add(2);
    collector1.add(2);
    collector1.add(2);
    collector1.add(2);
    collector1.add(2);
    collector1.add(2);
    Assert.assertEquals(Double.NaN, collector1.getKurtosis(), 0.0001);
  }
}