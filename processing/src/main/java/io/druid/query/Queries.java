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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaResultValue;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class Queries
{
  private static final Logger LOG = new Logger(Queries.class);

  public static void verifyAggregations(
      List<AggregatorFactory> aggFactories,
      List<PostAggregator> postAggs
  )
  {
    Preconditions.checkNotNull(aggFactories, "aggregations cannot be null");
    Preconditions.checkArgument(aggFactories.size() > 0, "Must have at least one AggregatorFactory");

    final Set<String> aggNames = Sets.newHashSet();
    for (AggregatorFactory aggFactory : aggFactories) {
      Preconditions.checkArgument(aggNames.add(aggFactory.getName()), "[%s] already defined", aggFactory.getName());
    }

    if (postAggs != null && !postAggs.isEmpty()) {
      final Set<String> combinedAggNames = Sets.newHashSet(aggNames);

      for (PostAggregator postAgg : postAggs) {
        final Set<String> dependencies = postAgg.getDependentFields();
        final Set<String> missing = Sets.difference(dependencies, combinedAggNames);

        Preconditions.checkArgument(
            missing.isEmpty(),
            "Missing fields [%s] for postAggregator [%s]", missing, postAgg.getName()
        );
        Preconditions.checkArgument(combinedAggNames.add(postAgg.getName()), "[%s] already defined", postAgg.getName());
      }
    }
  }

  public static <T> T convert(Object object, ObjectMapper jsonMapper, Class<T> expected)
  {
    try {
      return jsonMapper.convertValue(object, expected);
    }
    catch (Exception ex) {
      LOG.warn(ex, "Failed to convert to " + expected.getClass().getSimpleName());
    }
    return null;
  }

  public static Query toQuery(Map<String, Object> object, ObjectMapper jsonMapper)
  {
    return convert(object, jsonMapper, Query.class);
  }

  public static IncrementalIndexSchema relaySchema(Query subQuery, QuerySegmentWalker segmentWalker)
  {
    // use granularity truncated min timestamp since incoming truncated timestamps may precede timeStart
    IncrementalIndexSchema.Builder builder = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(Long.MIN_VALUE)
        .withQueryGranularity(QueryGranularities.ALL)
        .withFixedSchema(true);

    if (subQuery instanceof GroupByQuery) {
      final GroupByQuery groupByQuery = (GroupByQuery) subQuery;
      List<DimensionSpec> dimensions = groupByQuery.getDimensions();
      if (dimensions.isEmpty() && groupByQuery.getDataSource() instanceof ViewDataSource) {
        builder.withDimensions(resolveSchemaFromView(subQuery, segmentWalker).dimensions);
      } else {
        builder.withDimensions(Lists.transform(dimensions, DimensionSpec.OUTPUT_NAME));
      }
      List<AggregatorFactory> aggs = AggregatorFactory.toCombiner(groupByQuery.getAggregatorSpecs());
      for (PostAggregator postAggregator : groupByQuery.getPostAggregatorSpecs()) {
        aggs.add(new RelayAggregatorFactory(postAggregator.getName(), postAggregator.getName(), "object"));
      }
      return builder.withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()])).build();
    } else if (subQuery instanceof Query.ViewSupport) {
      final Query.ViewSupport<?> selectQuery = (Query.ViewSupport) subQuery;
      List<String> dimensions = Lists.transform(selectQuery.getDimensions(), DimensionSpec.OUTPUT_NAME);
      List<String> metrics = selectQuery.getMetrics();
      if (selectQuery.getDataSource() instanceof ViewDataSource) {
        ViewDataSource dataSource = (ViewDataSource) subQuery.getDataSource();
        List<String> columns = Lists.newArrayList(dataSource.getColumns());
        if (dimensions.isEmpty() && !columns.isEmpty()) {
          Schema schema = resolveSchemaFromView(subQuery, segmentWalker);
          return builder.withDimensions(schema.dimensions)
                        .withMetrics(AggregatorFactory.toRelay(schema.metrics))
                        .withRollup(false)
                        .build();
        } else {
          columns.removeAll(dimensions);
          if (metrics.isEmpty() && !columns.isEmpty()) {
            Schema schema = resolveSchemaFromView(subQuery, segmentWalker);
            return builder.withDimensions(schema.dimensions)
                          .withMetrics(AggregatorFactory.toRelay(schema.metrics))
                          .withRollup(false)
                          .build();
          }
        }
      }
      return builder.withDimensions(dimensions)
                    .withMetrics(AggregatorFactory.toRelay(metrics, "object"))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate joinQuery = (JoinQuery.JoinDelegate) subQuery;
      Set<String> dimensions = Sets.newLinkedHashSet();
      List<AggregatorFactory> metrics = Lists.newArrayList();
      for (Object query : joinQuery.getQueries()) {
        IncrementalIndexSchema schema = relaySchema((Query) query, segmentWalker);
        for (String dimension : schema.getDimensionsSpec().getDimensionNames()) {
          if (!dimensions.contains(dimension)) {
            dimensions.add(dimension);
          }
        }
        metrics.addAll(Arrays.asList(schema.getMetrics()));
      }
      return builder.withDimensions(Lists.newArrayList(dimensions))
                    .withMetrics(AggregatorFactory.toRelay(metrics))
                    .withRollup(false)
                    .build();
    } else {
      // todo union-all (partitioned-join, etc.)
      // todo timeseries topN query
      throw new UnsupportedOperationException("Cannot extract metric from query " + subQuery);
    }
  }

  private static class Schema
  {
    private final List<String> dimensions = Lists.newArrayList();
    private final List<AggregatorFactory> metrics = Lists.newArrayList();

    @Override
    public String toString()
    {
      return "Schema{" +
             "dimensions=" + dimensions +
             ", metrics=" + metrics +
             '}';
    }
  }

  private static Schema resolveSchemaFromView(Query<?> subQuery, QuerySegmentWalker segmentWalker)
  {
    Schema schema = new Schema();
    ViewDataSource dataSource = (ViewDataSource) subQuery.getDataSource();
    List<String> columns = dataSource.getColumns();
    List<String> exclusions = dataSource.getColumnExclusions();
    if (columns.isEmpty()) {
      return schema;
    }
    SelectMetaQuery metaQuery = new SelectMetaQuery(
        subQuery.getDataSource(),
        subQuery.getQuerySegmentSpec(),
        null,
        QueryGranularities.ALL,
        null,
        null,
        Maps.<String, Object>newHashMap()
    );
    Result<SelectMetaResultValue> result = Iterables.getOnlyElement(
        Sequences.toList(
            metaQuery.run(segmentWalker, Maps.<String, Object>newHashMap()),
            Lists.<Result<SelectMetaResultValue>>newArrayList()
        ), null
    );
    if (result == null) {
      return schema;
    }
    SelectMetaResultValue resolved = result.getValue();
    schema.dimensions.addAll(resolved.getDimensions());
    schema.dimensions.retainAll(columns);
    schema.dimensions.removeAll(exclusions);
    for (AggregatorFactory aggregator : resolved.getAggregators()) {
      if (!exclusions.contains(aggregator.getName()) && columns.contains(aggregator.getName())) {
        schema.metrics.add(aggregator);
      }
    }
    LOG.info("Resolved schema %s", schema);
    return schema;
  }

  public static <I> Function<I, Row> getRowConverter(Query<I> subQuery)
  {
    if (subQuery instanceof JoinQuery.JoinDelegate) {
      return new Function<I, Row>()
      {
        @Override
        @SuppressWarnings("unchecked")
        public Row apply(I input)
        {
          Map<String, Object> event = (Map<String, Object>) input;
          return new MapBasedRow((Long)event.get(EventHolder.timestampKey), event);
        }
      };
    }
    return GuavaUtils.caster();
  }
}
