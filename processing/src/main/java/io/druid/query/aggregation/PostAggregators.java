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

package io.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class PostAggregators
{
  public static List<PostAggregator> decorate(List<PostAggregator> aggregators, AggregatorFactory[] factories)
  {
    return decorate(aggregators, Arrays.asList(factories));
  }

  public static List<PostAggregator> decorate(List<PostAggregator> aggregators, List<AggregatorFactory> factories)
  {
    if (aggregators == null || aggregators.isEmpty()) {
      return ImmutableList.of();
    }
    Map<String, AggregatorFactory> mapping = Maps.newHashMap();
    for (AggregatorFactory factory : factories) {
      mapping.put(factory.getName(), factory);
    }
    List<PostAggregator> decorated = Lists.newArrayListWithExpectedSize(aggregators.size());
    for (PostAggregator aggregator : aggregators) {
      if (aggregator instanceof DecoratingPostAggregator) {
        aggregator = ((DecoratingPostAggregator) aggregator).decorate(mapping);
      }
      decorated.add(aggregator);
    }
    return decorated;
  }
}
