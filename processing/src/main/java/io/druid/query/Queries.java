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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.StreamQuery;

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

  public static Query toQuery(Map<String, Object> object, ObjectMapper jsonMapper)
  {
    try {
      return jsonMapper.convertValue(object, Query.class);
    }
    catch (Exception ex) {
      LOG.warn(ex, "Failed to convert to query");
    }
    return null;
  }

  public static List<DimensionSpec> getRelayDimensions(Query subQuery)
  {
    List<DimensionSpec> relay;
    if (subQuery instanceof GroupByQuery) {
      final GroupByQuery groupByQuery = (GroupByQuery) subQuery;
      relay = groupByQuery.getDimensions();
    } else if (subQuery instanceof SelectQuery) {
      final SelectQuery selectQuery = (SelectQuery) subQuery;
      relay = selectQuery.getDimensions();
    } else if (subQuery instanceof StreamQuery) {
      final StreamQuery streamQuery = (StreamQuery) subQuery;
      relay = streamQuery.getDimensions();
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final Set<DimensionSpec> dimensions = Sets.newHashSet();
      final JoinQuery.JoinDelegate joinQuery = (JoinQuery.JoinDelegate) subQuery;
      for (Object query : joinQuery.getQueries()) {
        dimensions.addAll(getRelayDimensions((Query)query));
      }
      return Lists.newArrayList(dimensions);
    } else {
      throw new UnsupportedOperationException("Cannot extract dimension from query " + subQuery);
    }
    if (relay.isEmpty() && subQuery.getDataSource() instanceof ViewDataSource) {
      for (String dimension : ((ViewDataSource)subQuery.getDataSource()).getColumns()) {
        relay.add(DefaultDimensionSpec.of(dimension));
      }
    }
    return relay;
  }

  public static List<AggregatorFactory> getRelayMetrics(Query subQuery)
  {
    final List<AggregatorFactory> relay = Lists.newArrayList();
    if (subQuery instanceof GroupByQuery) {
      final GroupByQuery groupByQuery = (GroupByQuery) subQuery;

      final Map<String, String> schema = Maps.newHashMap();
      for (AggregatorFactory aggregatorFactory : groupByQuery.getAggregatorSpecs()) {
        schema.put(aggregatorFactory.getName(), aggregatorFactory.getTypeName());
      }
      for (PostAggregator postAggregator : groupByQuery.getPostAggregatorSpecs()) {
        schema.put(postAggregator.getName(), "object");
      }
      for (Map.Entry<String, String> entry : schema.entrySet()) {
        relay.add(new RelayAggregatorFactory(entry.getKey(), entry.getKey(), entry.getValue()));
      }
    } else if (subQuery instanceof SelectQuery) {
      final SelectQuery selectQuery = (SelectQuery) subQuery;
      List<String> metrics = selectQuery.getMetrics();
      if (metrics.isEmpty()) {
        metrics = selectQuery.getDataSource() instanceof ViewDataSource ?
                  ((ViewDataSource)selectQuery.getDataSource()).getColumns() : metrics;
      }
      for (String metric : metrics) {
        relay.add(new RelayAggregatorFactory(metric, metric, "object"));
      }
    } else if (subQuery instanceof StreamQuery) {
      final StreamQuery streamQuery = (StreamQuery) subQuery;
      for (String metric : streamQuery.getMetrics()) {
        relay.add(new RelayAggregatorFactory(metric, metric, "object"));
      }
    } else if (subQuery instanceof JoinQuery.JoinDelegate) {
      final List<AggregatorFactory> aggregators = Lists.newArrayList();
      final JoinQuery.JoinDelegate joinQuery = (JoinQuery.JoinDelegate) subQuery;
      for (Object query : joinQuery.getQueries()) {
        aggregators.addAll(getRelayMetrics((Query)query));
      }
      return aggregators;
    } else {
      // todo union-all (partitioned-join, etc.)
      throw new UnsupportedOperationException("Cannot extract metric from query " + subQuery);
    }
    if (relay.isEmpty() && subQuery.getDataSource() instanceof ViewDataSource) {
      for (String metric : ((ViewDataSource)subQuery.getDataSource()).getColumns()) {
        relay.add(new RelayAggregatorFactory(metric, metric, "object"));
      }
    }
    return relay;
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
