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

package io.druid.query.kmeans;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.select.Schema;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryEngine;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class FindNearestQueryRunnerFactory
    implements QueryRunnerFactory<CentroidDesc, FindNearestQuery>
{
  private final FindNearestQueryToolChest toolChest;
  private final StreamQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public FindNearestQueryRunnerFactory(
      FindNearestQueryToolChest toolChest,
      StreamQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public Future<Object> preFactoring(FindNearestQuery query, List<Segment> segments, ExecutorService exec)
  {
    return Futures.<Object>immediateFuture(new MutableInt(0));
  }

  @Override
  public QueryRunner<CentroidDesc> createRunner(final Segment segment, final Future<Object> optimizer)
  {
    return new QueryRunner<CentroidDesc>()
    {
      @Override
      public Sequence<CentroidDesc> run(Query<CentroidDesc> query, Map<String, Object> responseContext)
      {
        FindNearestQuery nearestQuery = (FindNearestQuery) query;
        final int dimension = nearestQuery.getMetrics().size();

        final StreamQuery stream = nearestQuery.asInput();
        final StorageAdapter adapter = segment.asStorageAdapter(true);
        final Pair<Schema, Sequence<Object[]>> result = engine.processRaw(stream, adapter, optimizer, cache);

        final Centroid[] centroids = nearestQuery.getCentroids().toArray(new Centroid[0]);
        final CentroidDesc[] descs = new CentroidDesc[centroids.length];
        for (int i = 0; i < centroids.length; i++) {
          descs[i] = new CentroidDesc(i, dimension);
        }

        final double[] values = new double[dimension];

        result.rhs.accumulate(
            null, new Accumulator<CentroidDesc, Object[]>()
            {
              @Override
              public CentroidDesc accumulate(CentroidDesc accumulated, Object[] input)
              {
                for (int i = 0; i < dimension; i++) {
                  values[i] = ((Number) input[i]).doubleValue();
                }
                int nearest = -1;
                double minDistance = Double.MAX_VALUE;
                for (int i = 0; i < centroids.length; i++) {
                  double[] centroid = centroids[i].getCentroid();
                  double distance = 0;
                  for (int j = 0; j < dimension; j++) {
                    distance += Math.pow(centroid[j] - values[j], 2);
                  }
                  if (nearest < 0 || distance < minDistance) {
                    minDistance = distance;
                    nearest = i;
                  }
                }
                descs[nearest].add(values);
                return null;
              }
            }
        );
        return Sequences.simple(Arrays.asList(descs));
      }
    };
  }

  @Override
  public QueryRunner<CentroidDesc> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<CentroidDesc>> queryRunners,
      final Future<Object> optimizer
  )
  {
    return new ChainedExecutionQueryRunner<CentroidDesc>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<CentroidDesc, FindNearestQuery> getToolchest()
  {
    return toolChest;
  }
}
