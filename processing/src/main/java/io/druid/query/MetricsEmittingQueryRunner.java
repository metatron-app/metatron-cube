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

import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private final ServiceEmitter emitter;
  private final QueryToolChest<?, ? super Query<T>> queryToolChest;
  private final QueryRunner<T> queryRunner;
  private final long creationTimeNs;
  private final ObjLongConsumer<? super QueryMetrics<? super Query<T>>> reportMetric;
  private final Consumer<QueryMetrics<? super Query<T>>> applyCustomDimensions;

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<?, ? super Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      long creationTimeNs,
      ObjLongConsumer<? super QueryMetrics<? super Query<T>>> reportMetric,
      Consumer<QueryMetrics<? super Query<T>>> applyCustomDimensions
  )
  {
    this.emitter = emitter;
    this.queryToolChest = queryToolChest;
    this.queryRunner = queryRunner;
    this.creationTimeNs = creationTimeNs;
    this.reportMetric = reportMetric;
    this.applyCustomDimensions = applyCustomDimensions;
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<?, ? super Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      ObjLongConsumer<? super QueryMetrics<? super Query<T>>> reportMetric,
      Consumer<QueryMetrics<? super Query<T>>> applyCustomDimensions
  )
  {
    this(emitter, queryToolChest, queryRunner, -1, reportMetric, applyCustomDimensions);
  }


  public MetricsEmittingQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingQueryRunner<T>(
        emitter,
        queryToolChest,
        queryRunner,
        System.nanoTime(),
        reportMetric,
        applyCustomDimensions
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final QueryMetrics<? super Query<T>> queryMetrics = queryToolChest.makeMetrics(query);
    applyCustomDimensions.accept(queryMetrics);


    long startTimeNs = System.nanoTime();
    final Sequence<T> sequence = queryRunner.run(query, responseContext);
    final long elapsed = System.nanoTime() - startTimeNs;

    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType outType, Accumulator<OutType, T> accumulator)
      {
        OutType retVal;

        final long startTimeNs = System.nanoTime();
        try {
          retVal = sequence.accumulate(outType, accumulator);
        }
        catch (RuntimeException e) {
          queryMetrics.status("failed");
          throw e;
        }
        catch (Error e) {
          queryMetrics.status("failed");
          throw e;
        }
        finally {
          long timeTaken = elapsed + System.nanoTime() - startTimeNs;

          reportMetric.accept(queryMetrics, timeTaken);

          if (creationTimeNs > 0) {
            queryMetrics.reportWaitTime(startTimeNs - creationTimeNs);

          }
          queryMetrics.emit(emitter);
        }

        return retVal;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        Yielder<OutType> retVal;

        final long startTimeNs = System.nanoTime();
        try {
          retVal = sequence.toYielder(initValue, accumulator);
        }
        catch (RuntimeException e) {
          queryMetrics.status("failed");
          throw e;
        }
        catch (Error e) {
          queryMetrics.status("failed");
          throw e;
        }

        return makeYielder(startTimeNs, retVal, queryMetrics);
      }

      private <OutType> Yielder<OutType> makeYielder(
          final long startTimeNs,
          final Yielder<OutType> yielder,
          final QueryMetrics<? super Query<T>> queryMetrics)
      {
        return new Yielder<OutType>()
        {
          @Override
          public OutType get()
          {
            return yielder.get();
          }

          @Override
          public Yielder<OutType> next(OutType initValue)
          {
            try {
              return makeYielder(startTimeNs, yielder.next(initValue), queryMetrics);
            }
            catch (RuntimeException e) {
              queryMetrics.status("failed");
              throw e;
            }
            catch (Error e) {
              queryMetrics.status("failed");
              throw e;
            }
          }

          @Override
          public boolean isDone()
          {
            return yielder.isDone();
          }

          @Override
          public void close() throws IOException
          {
            try {
              long timeTaken = elapsed + System.nanoTime() - startTimeNs;
              reportMetric.accept(queryMetrics, timeTaken);


              if (creationTimeNs > 0) {
                queryMetrics.reportWaitTime(startTimeNs - creationTimeNs);
              }
            }
            finally {
              yielder.close();
              queryMetrics.emit(emitter);
            }
          }
        };
      }
    };
  }
}
