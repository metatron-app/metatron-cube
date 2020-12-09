/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Rows;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.UnionAllQueryRunner;
import io.druid.query.select.StreamQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Deprecated
public class KmeansTaggingPostProcessor<T> implements PostProcessingOperator.UnionSupport<T>
{
  private static final Logger LOG = new Logger(KmeansTaggingPostProcessor.class);

  @Override
  public QueryRunner postProcess(QueryRunner<T> baseQueryRunner)
  {
    throw new UnsupportedOperationException("should be used with union all query");
  }

  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner, final ExecutorService exec)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(final Query query, Map responseContext)
      {
        List<Pair<Query, Sequence>> sequences = Sequences.toList(baseRunner.run(query, responseContext));
        Preconditions.checkArgument(!sequences.isEmpty(), "should not be empty");
        Pair<Query, Sequence> first = sequences.remove(0);
        Preconditions.checkArgument(first.lhs instanceof KMeansQuery, "first should be kmeans query");
        final List<String> metrics = ((KMeansQuery)first.lhs).getMetrics();
        final Centroid[] centroids = Sequences.toArray(first.rhs, Centroid.class);
        final DistanceMeasure measure = DistanceMeasure.of(((KMeansQuery)first.lhs).getMeasure());
        final List<Sequence<Object[]>> tagged = Lists.newArrayList();
        for (Pair<Query, Sequence> pair : sequences) {
          if (pair.lhs instanceof StreamQuery) {
            StreamQuery stream = (StreamQuery) pair.lhs;
            List<String> outputColumns = stream.estimatedOutputColumns();
            if (outputColumns == null) {
              tagged.add(pair.rhs);
              continue;
            }
            final int[] indices = new int[metrics.size()];
            for (int i = 0; i < indices.length; i++) {
              int index = outputColumns.indexOf(metrics.get(i));
              if (index < 0) {
                tagged.add(pair.rhs);
                continue;
              }
              indices[i] = index;
            }
            final double[] point = new double[indices.length];
            tagged.add(
                Sequences.map(
                    (Sequence<Object[]>) pair.rhs, new Function<Object[], Object[]>()
                    {
                      @Override
                      public Object[] apply(final Object[] input)
                      {
                        for (int i = 0; i < indices.length; i++) {
                          point[i] = Rows.parseDouble(input[indices[i]]);
                        }
                        Object[] output = Arrays.copyOf(input, input.length + 1);
                        output[input.length] = measure.findNearest(centroids, point);
                        return output;
                      }
                    }
                )
            );
          } else {
            // todo
          }
        }
        return Sequences.concat(tagged);
      }
    };
  }
}
