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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.common.DateTimes;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 */
public class HyperUniqueFinalizingPostAggregatorTest
{
  private final HashFunction fn = Hashing.murmur3_128();

  @Test
  public void testCompute() throws Exception
  {
    Random random = new Random(0L);
    PostAggregator.Processor postAggregator = new HyperUniqueFinalizingPostAggregator("uniques", "uniques").processor();
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < 100; ++i) {
      byte[] hashedVal = fn.hashLong(random.nextLong()).asBytes();
      collector.add(hashedVal);
    }

    DateTime timestamp = DateTimes.nowUtc();
    double cardinality = (Double) postAggregator.compute(timestamp, ImmutableMap.<String, Object>of("uniques", collector));

    Assert.assertTrue(cardinality == 99.37233005831612);
  }
}
