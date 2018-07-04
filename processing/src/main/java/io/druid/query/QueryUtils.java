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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.metadata.metadata.NoneColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaResultValue;
import io.druid.query.spec.QuerySegmentSpec;
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

  public static <T> Sequence<T> mergeSort(Query<T> query, List<Sequence<T>> sequences)
  {
    if (sequences.isEmpty()) {
      return Sequences.empty();
    }
    if (sequences.size() == 1) {
      return sequences.get(0);
    }
    Ordering<T> ordering = query.getResultOrdering();
    return ordering == null ? Sequences.concat(sequences) : Sequences.mergeSort(ordering, Sequences.simple(sequences));
  }

  public static <T> Sequence<T> mergeSort(Query<T> query, Sequence<Sequence<T>> sequences)
  {
    Ordering<T> ordering = query.getResultOrdering();
    return ordering == null ? Sequences.concat(sequences) : Sequences.mergeSort(ordering, sequences);
  }

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
    // default.. regard skewed
    Map<String, Object> postProc = ImmutableMap.<String, Object>of(
        "type", "sketch.quantiles",
        "op", "QUANTILES",
        "slopedSpaced", slopedSpaced > 0 ? slopedSpaced + 1 : -1,
        "evenCounted", evenCounted > 0 ? evenCounted : -1
    );
    Query.MetricSupport query = (Query.MetricSupport) Queries.toQuery(
        ImmutableMap.<String, Object>builder()
                    .put("queryType", "sketch")
                    .put("dataSource", Queries.convert(dataSource, jsonMapper, Map.class))
                    .put("intervals", baseQuery.getQuerySegmentSpec())
                    .put("sketchOp", "QUANTILE")
                    .put("context", ImmutableMap.of(QueryContextKeys.POST_PROCESSING, postProc))
                    .build(), jsonMapper
    );

    if (query == null) {
      return null;
    }
    if (BaseQuery.getDimFilter(baseQuery) != null) {
      query = (Query.MetricSupport) query.withDimFilter(BaseQuery.getDimFilter(baseQuery));
    }
    query = query.withMetrics(Arrays.asList(column));

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

  public static Query setQueryId(Query query, final String queryId)
  {
    return Queries.iterate(
        query, new Function<Query, Query>()
        {
          @Override
          public Query apply(Query input)
          {
            if (input.getId() == null) {
              input = input.withId(queryId);
            }
            return input;
          }
        }
    );
  }

  public static Query resolveRecursively(Query query, final QuerySegmentWalker segmentWalker)
  {
    return Queries.iterate(
        query, new Function<Query, Query>()
        {
          @Override
          public Query apply(Query input)
          {
            return QueryUtils.resolveQuery(input, segmentWalker);
          }
        }
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> Query<T> resolveQuery(Query<T> query, QuerySegmentWalker segmentWalker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource && ((QueryDataSource) dataSource).getSchema() == null) {
      // cannot be resolved before sub-query is executed
      return query;
    }
    ViewDataSource view = ViewDataSource.of("dummy");
    if (dataSource instanceof ViewDataSource) {
      view = (ViewDataSource) dataSource;
      query = query.withDataSource(TableDataSource.of(view.getName()));
    }
    if (query instanceof Query.VCSupport && !view.getVirtualColumns().isEmpty()) {
      Query.VCSupport<T> vcSupport = (Query.VCSupport) query;
      query = vcSupport.withVirtualColumns(GuavaUtils.concat(view.getVirtualColumns(), vcSupport.getVirtualColumns()));
    }
    if (query instanceof Query.DimFilterSupport && view.getFilter() != null) {
      Query.DimFilterSupport<T> filterSupport = (Query.DimFilterSupport) query;
      query = filterSupport.withDimFilter(DimFilters.and(view.getFilter(), filterSupport.getDimFilter()));
    }

    Supplier<RowResolver> schema = QueryUtils.resolverSupplier(query, segmentWalker);
    query = query.resolveQuery(schema);

    if (query instanceof Query.ColumnsSupport) {
      Query.ColumnsSupport<T> columnsSupport = (Query.ColumnsSupport) query;
      if (GuavaUtils.isNullOrEmpty(columnsSupport.getColumns())) {
        query = view.getColumns().isEmpty() ?
                columnsSupport.withColumns(schema.get().getColumnNames()) :
                columnsSupport.withColumns(view.getColumns());
      }
    }
    if (query instanceof Query.DimensionSupport) {
      Query.DimensionSupport<T> dimSupport = (Query.DimensionSupport) query;
      if (dimSupport.getDimensions().isEmpty() && dimSupport.allDimensionsForEmpty()) {
        List<String> dimensions = GuavaUtils.retain(schema.get().getDimensionNames(), view.getColumns());
        query = dimSupport.withDimensionSpecs(DefaultDimensionSpec.toSpec(dimensions));
      }
    }
    if (query instanceof Query.MetricSupport) {
      Query.MetricSupport metricSupport = (Query.MetricSupport) query;
      if (metricSupport.getMetrics().isEmpty() && metricSupport.allMetricsForEmpty()) {
        List<String> metrics = GuavaUtils.retain(schema.get().getMetricNames(), view.getColumns());
        query = metricSupport.withMetrics(metrics);
      }
    }
    if (query instanceof Query.AggregationsSupport) {
      Query.AggregationsSupport aggrSupport = (Query.AggregationsSupport) query;
      if (aggrSupport.getAggregatorSpecs().isEmpty() && aggrSupport.allMetricsForEmpty()) {
        List<AggregatorFactory> factories = schema.get().getAggregatorsList();
        if (!factories.isEmpty() && !GuavaUtils.isNullOrEmpty(view.getColumns())) {
          Map<String, AggregatorFactory> map = AggregatorFactory.asMap(factories);
          factories = Lists.newArrayList(GuavaUtils.retain(map, view.getColumns()).values());
        }
        query = aggrSupport.withDimensionSpecs(factories);
      }
    }
    return query;
  }

  public static Supplier<Schema> schemaSupplier(
      final Query query,
      final QuerySegmentWalker segmentWalker
  )
  {
    return Suppliers.memoize(
        new Supplier<Schema>()
        {
          @Override
          public Schema get()
          {
            return retrieveSchema(query, segmentWalker);
          }
        }
    );
  }

  public static Supplier<RowResolver> resolverSupplier(
      final Query query,
      final QuerySegmentWalker segmentWalker)
  {
    return Suppliers.memoize(
        new Supplier<RowResolver>()
        {
          @Override
          public RowResolver get()
          {
            final Schema schema = retrieveSchema(query, segmentWalker);
            return RowResolver.of(schema, BaseQuery.getVirtualColumns(query));
          }
        }
    );
  }

  public static Schema retrieveSchema(Query<?> query, QuerySegmentWalker segmentWalker)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      Schema schema = ((QueryDataSource) dataSource).getSchema();
      return Preconditions.checkNotNull(schema, "schema of subquery is null");
    }
    if (dataSource instanceof ViewDataSource) {
      query.withDataSource(TableDataSource.of(((ViewDataSource)dataSource).getName()));
    }
    return getSchema(query, segmentWalker);
  }

  public static Schema getSchema(Query<?> query, QuerySegmentWalker segmentWalker)
  {
    SelectMetaQuery metaQuery = SelectMetaQuery.forSchema(
        query.getDataSource(),
        query.getQuerySegmentSpec(),
        query.getId()
    );
    Result<SelectMetaResultValue> result = Iterables.getOnlyElement(
        Sequences.toList(
            metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
            Lists.<Result<SelectMetaResultValue>>newArrayList()
        ), null
    );
    return result == null ? Schema.EMPTY : result.getValue().getSchema();
  }

  // nasty..
  public static boolean coveredBy(Query<?> query1, Query<?> query2)
  {
    final Granularity granularity = query1.getGranularity();
    final QuerySegmentSpec spec1 = query1.getQuerySegmentSpec();
    final QuerySegmentSpec spec2 = query2.getQuerySegmentSpec();

    List<Interval> intervals1 = spec1.getIntervals();
    if (granularity != null && granularity != Granularities.ALL) {
      intervals1 = Lists.newArrayList(
          Iterables.transform(
              intervals1, new Function<Interval, Interval>()
              {
                @Override
                public Interval apply(Interval input)
                {
                  return Intervals.of(
                      granularity.bucketStart(input.getStart()),
                      granularity.bucketStart(input.getEnd())
                  );
                }
              }
          )
      );
    }
    List<Interval> intervals2 = spec2.getIntervals();
    if (intervals1.size() == 1 && intervals2.size() == 1) {
      return intervals2.get(0).contains(intervals1.get(0));
    }
    // todo
    return intervals2.equals(intervals1);
  }
}
