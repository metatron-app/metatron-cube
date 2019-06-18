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

package io.druid.query.aggregation.area;

import org.junit.Assert;
import org.junit.Test;

public class MetricAreaTest
{
  public MetricAreaTest()
  {

  }

  @Test
  public void testSimple()
  {
    MetricArea metricArea = new MetricArea();

    double[] data = {0.1, 0.8, 0.3, 0.6, 0.5, 0.2, 0.1, 0.7};

    for (double val : data)
    {
      metricArea.add(val);
    }

    Assert.assertEquals(3.3, metricArea.sum, 0.001);
    Assert.assertEquals(8, metricArea.count);
    Assert.assertEquals(0.1, metricArea.min, 0.001);
    Assert.assertEquals(2.5, metricArea.getArea(), 0.001);
  }

  @Test
  public void testSimple2()
  {
    MetricArea metricArea = new MetricArea();

    double[] data = {1.4633397138E9, 1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9};

    for (double val: data) {
      metricArea.add(val);
    }

    Assert.assertEquals(0, metricArea.getArea(), 0.001);
  }

  @Test
  public void testAdd()
  {
    MetricArea ma1 = new MetricArea(20, 10, 1);
    MetricArea ma2 = new MetricArea(30, 15, 2);

    MetricArea ma3 = new MetricArea(50, 25, 1);

    Assert.assertEquals(ma3, ma1.add(ma2));
  }
}
