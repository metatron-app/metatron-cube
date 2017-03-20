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

package io.druid.query.aggregation.corr;

import org.junit.Assert;
import org.junit.Test;


public class PearsonAggregatorCollectorTest
{
  @Test
  public void testBasic()
  {
    PearsonAggregatorCollector collector1 = new PearsonAggregatorCollector();
    collector1.add(1, 2);
    collector1.add(2, 4);
    collector1.add(3, 6);
    collector1.add(4, 8);
    collector1.add(5, 10);
    collector1.add(6, 12);
    Assert.assertEquals(1.0, collector1.getCorr(), 0.0001);

    PearsonAggregatorCollector collector2 = new PearsonAggregatorCollector();
    collector2.add(1, 2);
    collector2.add(2, 3);
    collector2.add(3, 4);
    collector2.add(4, 3);
    collector2.add(5, 4);
    collector2.add(6, 7);
    Assert.assertEquals(0.8379, collector2.getCorr(), 0.0001);

    PearsonAggregatorCollector collector3 = PearsonAggregatorCollector.combineValues(collector1, collector2);
    Assert.assertEquals(0.7647, collector3.getCorr(), 0.0001);

    PearsonAggregatorCollector collector4 = PearsonAggregatorCollector.combineValues(collector2, collector1);
    Assert.assertEquals(0.7237, collector4.getCorr(), 0.0001);
  }

  @Test
  public void testConstant()
  {
    PearsonAggregatorCollector collector1 = new PearsonAggregatorCollector();
    collector1.add(1, 2);
    collector1.add(2, 2);
    collector1.add(3, 2);
    collector1.add(4, 2);
    collector1.add(5, 2);
    collector1.add(6, 2);
    Assert.assertEquals(Double.NaN, collector1.getCorr(), 0.0001);
  }
}