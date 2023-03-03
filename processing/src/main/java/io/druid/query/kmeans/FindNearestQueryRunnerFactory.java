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

package io.druid.query.kmeans;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.druid.cache.SessionCache;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.RowResolver;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryEngine;
import io.druid.segment.Segment;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class FindNearestQueryRunnerFactory
    extends QueryRunnerFactory.Abstract<CentroidDesc>
{
  private final StreamQueryEngine engine;
  private final QueryConfig config;

  @Inject
  public FindNearestQueryRunnerFactory(
      FindNearestQueryToolChest toolChest,
      StreamQueryEngine engine,
      QueryConfig config,
      QueryWatcher queryWatcher
  )
  {
    super(toolChest, queryWatcher);
    this.engine = engine;
    this.config = config;
  }

  @Override
  public Supplier<Object> preFactoring(
      Query<CentroidDesc> query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    return Suppliers.ofInstance(new MutableInt(0));
  }

  @Override
  public QueryRunner<CentroidDesc> _createRunner(Segment segment, Supplier<Object> optimizer, SessionCache cache)
  {
    return new QueryRunner<CentroidDesc>()
    {
      @Override
      public Sequence<CentroidDesc> run(Query<CentroidDesc> query, Map<String, Object> responseContext)
      {
        FindNearestQuery nearestQuery = (FindNearestQuery) query;
        final int dimension = nearestQuery.getMetrics().size();

        final StreamQuery stream = nearestQuery.asInput();
        final Sequence<Object[]> result = engine.process(stream, config, segment, optimizer, cache);

        final Centroid[] centroids = nearestQuery.getCentroids().toArray(new Centroid[0]);
        final CentroidDesc[] descs = new CentroidDesc[centroids.length];
        for (int i = 0; i < centroids.length; i++) {
          descs[i] = new CentroidDesc(i, dimension);
        }

        final double[] values = new double[dimension];
        final DistanceMeasure measure = DistanceMeasure.of(nearestQuery.getMeasure());
        final double maxDistance = nearestQuery.getMaxDistance();

        result.accumulate(
            null, new Accumulator<CentroidDesc, Object[]>()
            {
              @Override
              public CentroidDesc accumulate(CentroidDesc accumulated, Object[] input)
              {
                for (int i = 0; i < dimension; i++) {
                  values[i] = ((Number) input[i]).doubleValue();
                }
                final int nearest = measure.findNearest(centroids, values, maxDistance);
                if (nearest >= 0) {
                  descs[nearest].add(values);
                }
                return null;
              }
            }
        );
        return Sequences.simple(Arrays.asList(descs));
      }
    };
  }
}
