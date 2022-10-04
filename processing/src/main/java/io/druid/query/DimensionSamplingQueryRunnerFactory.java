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

package io.druid.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class DimensionSamplingQueryRunnerFactory extends QueryRunnerFactory.Abstract<Object[], DimensionSamplingQuery>
{
  private static final Logger LOG = new Logger(DimensionSamplingQueryRunnerFactory.class);

  @Inject
  public DimensionSamplingQueryRunnerFactory(DimensionSamplingQueryToolChest toolChest, QueryWatcher queryWatcher)
  {
    super(toolChest, queryWatcher);
  }

  @Override
  public Supplier<Object> preFactoring(
      DimensionSamplingQuery query,
      List<Segment> segments,
      Supplier<RowResolver> resolver,
      ExecutorService exec
  )
  {
    int numSample = (int) (segments.stream().mapToInt(s -> s.getNumRows()).sum() * query.getSampleRatio());
    return Suppliers.ofInstance(new Sampler(Math.max(10, numSample), query.getDimensions()));
  }

  @Override
  public QueryRunner<Object[]> mergeRunners(
      final Query<Object[]> query,
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Object[]>> runners,
      final Supplier<Object> optimizer
  )
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> query, Map<String, Object> responseContext)
      {
        long start = System.currentTimeMillis();
        for (QueryRunner<Object[]> runner : runners) {
          runner.run(query, responseContext);
        }
        long time = System.currentTimeMillis() - start;
        Sampler sampler = (Sampler) optimizer.get();
        LOG.info("Sampled %d rows and skipped %d rows, took %,d msec", sampler.sampled, sampler.skipped, time);
        return sampler.build();
      }
    };
  }

  @Override
  public QueryRunner<Object[]> _createRunner(final Segment segment, Supplier<Object> optimizer)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query<Object[]> base, Map<String, Object> responseContext)
      {
        final Sampler sampler = (Sampler) optimizer.get();
        final DimensionSamplingQuery query = (DimensionSamplingQuery) base;
        final RowResolver resolver = RowResolver.of(segment, query.getVirtualColumns());
        final DimFilter filter = query.getFilter();

        segment.asStorageAdapter(true)
               .makeCursors(filter, segment.getInterval(), resolver, Granularities.ALL, false, cache)
               .accumulate(sampler, sampler);
        return Sequences.empty();
      }
    };
  }

  private static class Sampler implements Accumulator<Sampler, Cursor>
  {
    private final Random r = new Random();
    private final List<DimensionSpec> dimensions;

    private int index;
    private final Object[][] sample;
    private final double sampleSizeReverse;
    private double W;
    private int skip;
    private int sampled;
    private int skipped;

    private Sampler(int sampling, List<DimensionSpec> dimensions)
    {
      this.dimensions = dimensions;
      this.sample = new Object[sampling][dimensions.size()];
      this.sampleSizeReverse = 1.0 / sampling;
      this.W = Math.pow(r.nextDouble(), sampleSizeReverse);
    }

    @Override
    public Sampler accumulate(Sampler accumulated, Cursor cursor)
    {
      if (skip > 0) {
        skip = cursor.advanceNWithoutMatcher(skip);
        if (skip > 0) {
          return this;
        }
        cursor.advance();
      }
      if (cursor.isDone()) {
        return this;
      }
      final List<DimensionSelector> selectors =
          GuavaUtils.transform(dimensions, dimension -> cursor.makeDimensionSelector(dimension));
      for (; !cursor.isDone(); cursor.advance(), sampled++) {
        if (index < sample.length) {
          select(selectors, sample[index++]);
        } else {
          select(selectors, sample[r.nextInt(sample.length)]);
          skip = (int) (Math.log(r.nextDouble()) / Math.log(1 - W));
          if (skip > 0) {
            skipped += skip;
            skip = cursor.advanceNWithoutMatcher(skip);
          }
          if (skip == 0) {
            W *= Math.pow(r.nextDouble(), sampleSizeReverse);
          }
        }
      }
      return this;
    }

    private void select(List<DimensionSelector> selectors, Object[] array)
    {
      for (int i = 0; i < array.length; i++) {
        DimensionSelector selector = selectors.get(i);
        array[i] = selector.lookupName(selector.getRow().get(0));    // todo muti-value?
      }
    }

    private Sequence<Object[]> build()
    {
      return index < sample.length ? Sequences.of(Arrays.copyOfRange(sample, 0, index)) : Sequences.of(sample);
    }
  }
}
