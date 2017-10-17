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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.ColumnAnalysis;
import io.druid.query.metadata.metadata.ColumnIncluderator;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumn;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
      DataSource dataSource,
      String column,
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

    ViewDataSource view;
    if (dataSource instanceof ViewDataSource) {
      view = (ViewDataSource) dataSource;
      view = view.withColumns(Arrays.asList(column))
                 .withFilter(DimFilters.andNullable(view.getFilter(), filter));
    } else {
      String name = Iterables.getOnlyElement(dataSource.getNames());
      view = new ViewDataSource(name, Arrays.asList(column), null, filter, false);
    }
    Query.DimFilterSupport query = (Query.DimFilterSupport) Queries.toQuery(
        ImmutableMap.<String, Object>builder()
                    .put("queryType", "sketch")
                    .put("dataSource", Queries.convert(view, jsonMapper, Map.class))
                    .put("intervals", segmentSpec)
                    .put("sketchOp", "QUANTILE")
                    .put("context", ImmutableMap.of(QueryContextKeys.POST_PROCESSING, postProc))
                    .build(), jsonMapper
    );

    if (query == null) {
      return null;
    }
    final Query runner = query.withId(UUID.randomUUID().toString());

    log.info("Running sketch query on join partition key %s.%s", dataSource, column);
    log.debug("Running.. %s", runner);

    @SuppressWarnings("unchecked")
    final List<Result<Map<String, Object>>> res = Sequences.toList(
        runner.run(segmentWalker, Maps.newHashMap()), Lists.<Result<Map<String, Object>>>newArrayList()
    );
    if (!res.isEmpty()) {
      String prev = null;
      String[] splits = (String[]) res.get(0).getValue().get(column);
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

  public static List<Interval> analyzeInterval(QuerySegmentWalker segmentWalker, Query<?> query)
  {
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        null,
        null,
        new NoneColumnIncluderator(),
        false,
        Queries.extractContext(query, BaseQuery.QUERYID),
        EnumSet.of(SegmentMetadataQuery.AnalysisType.INTERVAL),
        false,
        false
    );
    @SuppressWarnings("unchecked")
    final List<SegmentAnalysis> res = Sequences.toList(
        metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()), Lists.<SegmentAnalysis>newArrayList()
    );
    Preconditions.checkArgument(res.size() == 1);
    return res.get(0).getIntervals();
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Map<String, MutableInt>> analyzeTypes(QuerySegmentWalker segmentWalker, Query query)
  {
    List<VirtualColumn> vcs = null;
    if (query instanceof Query.DimensionSupport) {
      vcs = ((Query.DimensionSupport)query).getVirtualColumns();
    }
    SegmentMetadataQuery metaQuery = new SegmentMetadataQuery(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        vcs,
        null,
        ColumnIncluderator.ALL,
        false,
        Queries.extractContext(query, BaseQuery.QUERYID),
        EnumSet.noneOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        false
    );
    List<Interval> intervals = metaQuery.getQuerySegmentSpec().getIntervals();
    QueryRunner<SegmentAnalysis> runner = segmentWalker.getQueryRunnerForIntervals(metaQuery, intervals);

    Sequence<SegmentAnalysis> sequence = runner.run(metaQuery, Maps.<String, Object>newHashMap());
    final Map<String, Map<String, MutableInt>> results = Maps.newHashMap();
    sequence.accumulate(
        null, new Accumulator<Object, SegmentAnalysis>()
        {
          @Override
          public Object accumulate(Object accumulated, SegmentAnalysis in)
          {
            for (Map.Entry<String, ColumnAnalysis> entry : in.getColumns().entrySet()) {
              Map<String, MutableInt> counters = results.get(entry.getKey());
              if (counters == null) {
                results.put(entry.getKey(), counters = Maps.newHashMap());
              }
              String type = entry.getValue().getType();
              MutableInt counter = counters.get(type);
              if (counter == null) {
                counters.put(type, counter = new MutableInt());
              }
              counter.increment();
            }
            return accumulated;
          }
        }
    );
    return results;
  }

  public static Map<String, String> toMajorType(Map<String, Map<String, MutableInt>> types)
  {
    Map<String, String> majorTypes = Maps.newHashMap();
    for (Map.Entry<String, Map<String, MutableInt>> entry : types.entrySet()) {
      String column = entry.getKey();
      Map<String, MutableInt> value = entry.getValue();
      if (value.isEmpty()) {
        continue;
      }
      if (value.size() == 1) {
        majorTypes.put(column, Iterables.getOnlyElement(value.keySet()));
        continue;
      }
      int max = -1;
      String major = null;
      for (Map.Entry<String, MutableInt> x : value.entrySet()) {
        int count = x.getValue().intValue();
        if (max < 0 || max <= count) {
          major = x.getKey();
          max = count;
        }
      }
      majorTypes.put(column, major);
    }
    return majorTypes;
  }

  public static Iterable<DimFilter> toFilters(final String column, final List<String> partitions)
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
              filter = BoundDimFilter.lt(column, partitions.get(index));
            } else if (index == partitions.size() - 1) {
              filter = BoundDimFilter.gte(column, partitions.get(index - 1));
            } else {
              filter = BoundDimFilter.between(column, partitions.get(index - 1), partitions.get(index));
            }
            index++;
            return filter;
          }
        };
      }
    };
  }
}
