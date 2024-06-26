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

package io.druid.query.aggregation.range;

import org.junit.Assert;
import org.junit.Test;

public class MetricRangeTest
{
  public MetricRangeTest()
  {

  }

  @Test
  public void testSimple()
  {
    MetricRange mr = new MetricRange();

    double[] data = {0.1, 0.8, 0.3, 0.6, 0.5, 0.2, 0.1, 0.7};

    for (double val : data)
    {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(0.1, 0.8);

    Assert.assertEquals(expected, mr);
    Assert.assertEquals(0.7, mr.getRange(), 0.001);
  }

  @Test
  public void testSimple2()
  {
    MetricRange mr = new MetricRange();

    double tmp = Double.parseDouble("1.4633397138E9");

    double[] data = {1.4633397138E9, 1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9,1.4633397138E9};

    for (double val : data)
    {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(1463339713.8, 1463339713.8);

    Assert.assertEquals(expected, mr);
    Assert.assertEquals(0, mr.getRange(), 0.001);
  }

  @Test
  public void testSimple3()
  {
    MetricRange mr = new MetricRange();

    double[] data = {-2, -2, -2, -2};

    for (double val: data) {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(-2, -2);

    Assert.assertEquals(expected, mr);
  }

  @Test
  public void testNegative()
  {
    MetricRange mr = new MetricRange();

    double[] data = {-0.47498724,
        -1.5028697,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
        -1.4999698,
    };

    for (double val: data) {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(-1.5028697, -0.47498724);
    MetricRange mr2 = new MetricRange();
    mr2.add(mr);
    MetricRange mr3 = new MetricRange();
    mr.add(mr3);
    double range = mr2.getRange();

    Assert.assertEquals(expected, mr);

  }

  @Test
  public void testAdd()
  {
    MetricRange mr1 = new MetricRange(0, 10);
    MetricRange mr2 = new MetricRange(20, 30);

    MetricRange mr3 = new MetricRange(0, 30);

    Assert.assertEquals(mr3, mr1.add(mr2));
  }
}
