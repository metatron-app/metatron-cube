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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class PostAggregators
{
  public static List<String> toNames(List<PostAggregator> aggregators)
  {
    List<String> names = Lists.newArrayList();
    for (PostAggregator postAggregator : aggregators) {
      names.add(postAggregator.getName());
    }
    return names;
  }

  public static String[] toNamesAsArray(List<PostAggregator> aggregators)
  {
    return toNames(aggregators).toArray(new String[aggregators.size()]);
  }

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
      if (aggregator instanceof PostAggregator.Decorating) {
        aggregator = ((PostAggregator.Decorating) aggregator).decorate(mapping);
      }
      decorated.add(aggregator);
    }
    return decorated;
  }

  public static List<PostAggregator.Processor> toProcessors(List<PostAggregator> postAggregators)
  {
    if (GuavaUtils.isNullOrEmpty(postAggregators)) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(Iterables.transform(
        postAggregators,
        new Function<PostAggregator, PostAggregator.Processor>()
        {
          @Override
          public PostAggregator.Processor apply(PostAggregator postAggregator)
          {
            return postAggregator.processor();
          }
        }
    ));
  }

  public abstract static class MapAccess extends AbstractMap<String, Object>
  {
    @Override
    public Set<Entry<String, Object>> entrySet()
    {
      throw new UnsupportedOperationException("entrySet");
    }
  }
}
