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

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.select.Schema;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryEngine;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class FindNearestQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<CentroidDesc, FindNearestQuery>
{
  private final StreamQueryEngine engine;

  @Inject
  public FindNearestQueryRunnerFactory(
      FindNearestQueryToolChest toolChest,
      StreamQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
  }

  @Override
  public Future<Object> preFactoring(
      FindNearestQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
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
        final Pair<Schema, Sequence<Object[]>> result = engine.processRaw(stream, segment, optimizer, cache);

        final Centroid[] centroids = nearestQuery.getCentroids().toArray(new Centroid[0]);
        final CentroidDesc[] descs = new CentroidDesc[centroids.length];
        for (int i = 0; i < centroids.length; i++) {
          descs[i] = new CentroidDesc(i, dimension);
        }

        final double[] values = new double[dimension];
        final DistanceMeasure measure = DistanceMeasure.of(nearestQuery.getMeasure());

        result.rhs.accumulate(
            null, new Accumulator<CentroidDesc, Object[]>()
            {
              @Override
              public CentroidDesc accumulate(CentroidDesc accumulated, Object[] input)
              {
                for (int i = 0; i < dimension; i++) {
                  values[i] = ((Number) input[i]).doubleValue();
                }
                int nearest = measure.findNearest(centroids, values);
                descs[nearest].add(values);
                return null;
              }
            }
        );
        return Sequences.simple(Arrays.asList(descs));
      }
    };
  }
}
