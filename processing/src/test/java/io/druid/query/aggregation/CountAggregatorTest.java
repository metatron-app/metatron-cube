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

package io.druid.query.aggregation;

import org.apache.commons.lang.mutable.MutableLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;

/**
 */
public class CountAggregatorTest
{
  @Test
  public void testAggregate()
  {
    CountAggregator agg = new CountAggregator();

    MutableLong x = null;
    Assert.assertEquals(0L, agg.get(x));
    Assert.assertEquals(0L, agg.get(x));
    Assert.assertEquals(0L, agg.get(x));
    x = agg.aggregate(x);
    Assert.assertEquals(1L, agg.get(x));
    Assert.assertEquals(1L, agg.get(x));
    Assert.assertEquals(1L, agg.get(x));
    x = agg.aggregate(x);
    Assert.assertEquals(2L, agg.get(x));
    Assert.assertEquals(2L, agg.get(x));
    Assert.assertEquals(2L, agg.get(x));
  }

  @Test
  public void testComparator()
  {
    CountAggregator agg = new CountAggregator();

    MutableLong x = null;
    Object first = agg.get(x);
    x = agg.aggregate(x);

    Comparator comp = new CountAggregatorFactory("null").getComparator();

    Assert.assertEquals(-1, comp.compare(first, agg.get(x)));
    Assert.assertEquals(0, comp.compare(first, first));
    Assert.assertEquals(0, comp.compare(agg.get(x), agg.get(x)));
    Assert.assertEquals(1, comp.compare(agg.get(x), first));
  }
}
