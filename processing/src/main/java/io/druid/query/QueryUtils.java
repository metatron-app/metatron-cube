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
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.data.ValueDesc;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaResultValue;
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
      Query baseQuery,
      QuerySegmentWalker segmentWalker,
      ObjectMapper jsonMapper,
      String column,
      int slopedSpaced,
      int evenCounted
  )
  {
    DataSource dataSource = baseQuery.getDataSource();
    ViewDataSource view;
    if (dataSource instanceof ViewDataSource) {
      view = (ViewDataSource) dataSource;
      view = view.withColumns(Arrays.asList(column))
                 .withFilter(DimFilters.and(view.getFilter(), BaseQuery.getDimFilter(baseQuery)));
    } else {
      String name = Iterables.getOnlyElement(dataSource.getNames());
      view = new ViewDataSource(name, Arrays.asList(column), null, BaseQuery.getDimFilter(baseQuery), false);
    }
    // default.. regard skewed
    Map<String, Object> postProc = ImmutableMap.<String, Object>of(
        "type", "sketch.quantiles",
        "op", "QUANTILES",
        "slopedSpaced", slopedSpaced > 0 ? slopedSpaced + 1 : -1,
        "evenCounted", evenCounted > 0 ? evenCounted : -1
    );
    Query.DimFilterSupport query = (Query.DimFilterSupport) Queries.toQuery(
        ImmutableMap.<String, Object>builder()
                    .put("queryType", "sketch")
                    .put("dataSource", Queries.convert(view, jsonMapper, Map.class))
                    .put("intervals", baseQuery.getQuerySegmentSpec())
                    .put("sketchOp", "QUANTILE")
                    .put("context", ImmutableMap.of(QueryContextKeys.POST_PROCESSING, postProc))
                    .build(), jsonMapper
    );

    if (query == null) {
      return null;
    }
    final Query runner = query.withId(UUID.randomUUID().toString());

    log.info("Running sketch query on partition key %s.%s", dataSource, column);
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
  public static Map<String, Map<ValueDesc, MutableInt>> analyzeTypes(QuerySegmentWalker segmentWalker, Query query)
  {
    SelectMetaQuery metaQuery = SelectMetaQuery.forQuery(query, true);

    Sequence sequence = metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap());
    final Map<String, Map<ValueDesc, MutableInt>> results = Maps.newHashMap();
    sequence.accumulate(
        null, new Accumulator<Object, Result<BySegmentResultValue<Result<SelectMetaResultValue>>>>()
        {
          @Override
          public Object accumulate(Object accumulated, Result<BySegmentResultValue<Result<SelectMetaResultValue>>> in)
          {
            BySegmentResultValue<Result<SelectMetaResultValue>> bySegment = in.getValue();
            for (Result<SelectMetaResultValue> result : bySegment.getResults()) {
              SelectMetaResultValue value = result.getValue();
              for (Pair<String, ValueDesc> pair : value.getSchema().columnAndTypes()) {
                Map<ValueDesc, MutableInt> counters = results.get(pair.lhs);
                if (counters == null) {
                  results.put(pair.lhs, counters = Maps.newHashMap());
                }
                ValueDesc type = pair.rhs;
                MutableInt counter = counters.get(type);
                if (counter == null) {
                  counters.put(type, counter = new MutableInt());
                }
                counter.increment();
              }
            }
            return accumulated;
          }
        }
    );
    return results;
  }

  public static Map<String, ValueDesc> toMajorType(Map<String, Map<ValueDesc, MutableInt>> types)
  {
    Map<String, ValueDesc> majorTypes = Maps.newHashMap();
    for (Map.Entry<String, Map<ValueDesc, MutableInt>> entry : types.entrySet()) {
      String column = entry.getKey();
      Map<ValueDesc, MutableInt> value = entry.getValue();
      if (value.isEmpty()) {
        continue;
      }
      if (value.size() == 1) {
        majorTypes.put(column, Iterables.getOnlyElement(value.keySet()));
        continue;
      }
      int max = -1;
      ValueDesc major = null;
      for (Map.Entry<ValueDesc, MutableInt> x : value.entrySet()) {
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

  public static Schema resolveSchema(Query<?> query, QuerySegmentWalker segmentWalker)
  {
    if (query.getDataSource() instanceof QueryDataSource) {
      Schema schema = ((QueryDataSource) query.getDataSource()).getSchema();
      return Preconditions.checkNotNull(schema, "schema of subquery is null");
    }
    return getSchema(
        SelectMetaQuery.forSchema(
            query.getDataSource(),
            query.getQuerySegmentSpec(),
            query.getId()
        ), segmentWalker
    );
  }

  private static Schema getSchema(SelectMetaQuery metaQuery, QuerySegmentWalker segmentWalker)
  {
    Result<SelectMetaResultValue> result = Iterables.getOnlyElement(
        Sequences.toList(
            metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
            Lists.<Result<SelectMetaResultValue>>newArrayList()
        ), null
    );
    return result == null ? Schema.EMPTY : result.getValue().getSchema();
  }
}
