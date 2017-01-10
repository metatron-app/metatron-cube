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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryUtils
{
  private static final Logger log = new Logger(QueryUtils.class);

  public static List<String> runSketchQuery(
      QuerySegmentWalker segmentWalker,
      ObjectMapper jsonMapper,
      QuerySegmentSpec segmentSpec,
      DimFilter filter,
      String table,
      String expression,
      int evenSpaced,
      int evenCounted
  )
  {
    // default.. regard skewed
    Object postProc = ImmutableMap.<String, Object>of(
        "type", "sketch.quantiles",
        "op", "QUANTILES",
        "evenSpaced", evenSpaced > 0 ? evenSpaced + 1 : -1,
        "evenCounted", evenCounted > 0 ? evenCounted : -1
    );

    Query.DimFilterSupport query = (Query.DimFilterSupport) Queries.toQuery(
        ImmutableMap.<String, Object>builder()
                    .put("queryType", "sketch")
                    .put("dataSource", table)
                    .put("intervals", segmentSpec)
                    .put("dimensions", Arrays.asList(expression))
                    .put("sketchOp", "QUANTILE")
                    .put("context", ImmutableMap.of("postProcessing", postProc))
                    .build(), jsonMapper
    );

    if (query == null) {
      return null;
    }
    if (filter != null) {
      query = query.withDimFilter(filter);
    }
    log.info("Running sketch query on join partition key %s.%s", table, expression);
    log.debug("Running.. %s", query);

    @SuppressWarnings("unchecked")
    final List<Result<Map<String, Object>>> res = Sequences.toList(
        query.run(segmentWalker, Maps.newHashMap()), Lists.<Result<Map<String, Object>>>newArrayList()
    );
    if (!res.isEmpty()) {
      String prev = null;
      String[] splits = (String[]) res.get(0).getValue().get(expression);
      log.info("Partition keys.. %s", Arrays.toString(splits));
      List<String> partitions = Lists.newArrayList();
      for (String split : splits) {
        if (prev == null || !prev.equals(split)) {
          partitions.add(split);
        }
        prev = split;
      }
      return partitions;
    }
    return null;
  }

  public static Iterable<DimFilter> toFilters(final String expression, final List<String> partitions)
  {
    return new Iterable<DimFilter>()
    {
      @Override
      public Iterator<DimFilter> iterator()
      {
        return new Iterator<DimFilter>()
        {
          private int index = 1;

          @Override
          public boolean hasNext()
          {
            return index < partitions.size();
          }

          @Override
          public DimFilter next()
          {
            DimFilter filter;
            if (index == 1) {
              filter = BoundDimFilter.lt(expression, partitions.get(index));
            } else if (index == partitions.size() - 1) {
              filter = BoundDimFilter.gte(expression, partitions.get(index - 1));
            } else {
              filter = BoundDimFilter.between(expression, partitions.get(index - 1), partitions.get(index));
            }
            index++;
            return filter;
          }
        };
      }
    };
  }
}
