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
import io.druid.data.ValueDesc;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.PartitionedGroupByQuery;
import io.druid.query.select.EventHolder;
import io.druid.query.select.Schema;
import io.druid.query.select.SelectMetaQuery;
import io.druid.query.select.SelectMetaResultValue;
import io.druid.segment.incremental.IncrementalIndexSchema;

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
        builder.withDimensions(resolveSchemaFromView(subQuery, segmentWalker).getDimensionNames());
      } else {
        builder.withDimensions(DimensionSpecs.toOutputNames(dimensions));
      }
      List<AggregatorFactory> aggs = AggregatorFactory.toCombiner(groupByQuery.getAggregatorSpecs());
      for (PostAggregator postAggregator : groupByQuery.getPostAggregatorSpecs()) {
        aggs.add(new RelayAggregatorFactory(postAggregator.getName(), postAggregator.getName(), ValueDesc.UNKNOWN_TYPE));
      }
      return builder.withMetrics(aggs.toArray(new AggregatorFactory[aggs.size()])).build();
    } else if (subQuery instanceof Query.ViewSupport) {
      final Query.ViewSupport<?> selectQuery = (Query.ViewSupport) subQuery;
      List<String> dimensions = DimensionSpecs.toOutputNames(selectQuery.getDimensions());
      List<String> metrics = selectQuery.getMetrics();
      if (selectQuery.needsSchemaResolution()) {
        Schema schema = resolveSchemaFromView(subQuery, segmentWalker);
        return builder.withDimensions(schema.getDimensionNames())
                      .withMetrics(AggregatorFactory.toRelay(schema.metricAndTypes()))
                      .withRollup(false)
                      .build();
      }
      return builder.withDimensions(dimensions)
                    .withMetrics(AggregatorFactory.toRelay(metrics, ValueDesc.UNKNOWN_TYPE))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate<?> joinQuery = (JoinQuery.JoinDelegate) subQuery;
      List<String> dimensions = Lists.newArrayList();
      List<AggregatorFactory> metrics = Lists.newArrayList();
      List queries = joinQuery.getQueries();
      List<String> aliases = joinQuery.getPrefixAliases();
      for (int i = 0; i < queries.size(); i++) {
        final String prefix = aliases == null ? "" : aliases.get(i) + ".";
        IncrementalIndexSchema schema = relaySchema((Query) queries.get(i), segmentWalker);
        for (String dimension : schema.getDimensionsSpec().getDimensionNames()) {
          String prefixed = prefix + dimension;
          if (!dimensions.contains(prefixed)) {
            dimensions.add(prefixed);
          }
        }
        AggregatorFactory[] aggregators = schema.getMetrics();
        for (AggregatorFactory aggregator : aggregators) {
          String prefixed = prefix + aggregator.getName();
          Preconditions.checkArgument(aggregator instanceof RelayAggregatorFactory);
          metrics.add(new RelayAggregatorFactory(prefixed, prefixed, aggregator.getTypeName()));
        }
      }
      // multiple timestamps.. we use timestamp from the first alias as indexing timestamp and add others into metric
      if (aliases != null) {
        for (String alias : aliases) {
          String timestamp = alias + "." + EventHolder.timestampKey;
          metrics.add(new RelayAggregatorFactory(timestamp, timestamp, ValueDesc.LONG_TYPE));
        }
      }
      return builder.withDimensions(dimensions)
                    .withMetrics(AggregatorFactory.toRelay(Iterables.transform(metrics, AggregatorFactory.NAME_TYPE)))
                    .withRollup(false)
                    .build();
    } else if (subQuery instanceof PartitionedGroupByQuery.GroupByDelegate) {
      GroupByQuery query = (GroupByQuery) ((UnionAllQuery)subQuery).getQueries().get(0);
      return relaySchema(query, segmentWalker);
    } else {
      // todo union-all (partitioned-join, etc.)
      // todo timeseries topN query
      throw new UnsupportedOperationException("Cannot extract metric from query " + subQuery);
    }
  }

  private static Schema resolveSchemaFromView(Query<?> subQuery, QuerySegmentWalker segmentWalker)
  {
    SelectMetaQuery metaQuery = new SelectMetaQuery(
        subQuery.getDataSource(),
        subQuery.getQuerySegmentSpec(),
        null,
        QueryGranularities.ALL,
        null,
        null,
        null,
        true,
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
      return Schema.EMPTY;
    }
    return result.getValue().getSchema();
  }

  public static <I> Function<I, Row> getRowConverter(Query<I> subQuery)
  {
    if (subQuery instanceof JoinQuery.JoinDelegate) {
      final JoinQuery.JoinDelegate delegate = (JoinQuery.JoinDelegate) subQuery;
      final String timeColumn = delegate.getPrefixAliases() == null
                                ? EventHolder.timestampKey
                                : delegate.getPrefixAliases().get(0) + "." + EventHolder.timestampKey;
      return new Function<I, Row>()
      {
        @Override
        @SuppressWarnings("unchecked")
        public Row apply(I input)
        {
          Map<String, Object> event = (Map<String, Object>) input;
          return new MapBasedRow((Long)event.get(timeColumn), event);
        }
      };
    }
    return GuavaUtils.caster();
  }
}
