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

import io.druid.common.utils.Sequences;
import io.druid.common.utils.VMUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> delegate;
  private final QueryToolChest<T, ? extends Query<T>> queryToolChest;
  private final ServiceEmitter emitter;
  private final AtomicLong cpuTimeAccumulator;
  private final boolean report;

  private CPUTimeMetricQueryRunner(
      QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      ServiceEmitter emitter,
      AtomicLong cpuTimeAccumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      throw new ISE("Cpu time must enabled");
    }
    this.delegate = delegate;
    this.queryToolChest = queryToolChest;
    this.emitter = emitter;
    this.cpuTimeAccumulator = cpuTimeAccumulator == null ? new AtomicLong(0L) : cpuTimeAccumulator;
    this.report = report;
  }


  @Override
  public Sequence<T> run(
      final Query<T> query, final Map<String, Object> responseContext
  )
  {
    final Sequence<T> baseSequence = delegate.run(query, responseContext);
    return Sequences.withEffect(
        new Sequence<T>()
        {
          @Override
          public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
          {
            final long start = VMUtils.getCurrentThreadCpuTime();
            try {
              return baseSequence.accumulate(initValue, accumulator);
            }
            finally {
              cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
            }
          }

          @Override
          public <OutType> Yielder<OutType> toYielder(
              OutType initValue, YieldingAccumulator<OutType, T> accumulator
          )
          {
            final Yielder<OutType> delegateYielder = baseSequence.toYielder(initValue, accumulator);
            return new Yielder<OutType>()
            {
              @Override
              public OutType get()
              {
                final long start = VMUtils.getCurrentThreadCpuTime();
                try {
                  return delegateYielder.get();
                }
                finally {
                  cpuTimeAccumulator.addAndGet(
                      VMUtils.getCurrentThreadCpuTime() - start
                  );
                }
              }

              @Override
              public Yielder<OutType> next(OutType initValue)
              {
                final long start = VMUtils.getCurrentThreadCpuTime();
                try {
                  return delegateYielder.next(initValue);
                }
                finally {
                  cpuTimeAccumulator.addAndGet(
                      VMUtils.getCurrentThreadCpuTime() - start
                  );
                }
              }

              @Override
              public boolean isDone()
              {
                return delegateYielder.isDone();
              }

              @Override
              public void close() throws IOException
              {
                delegateYielder.close();
              }
            };
          }
        },
        new Runnable()
        {
          @Override
          public void run()
          {
            if (report) {
              final long cpuTimeNs = cpuTimeAccumulator.get();
              if (cpuTimeNs > 0) {
                QueryMetrics<?> queryMetrics = QueryToolChest.getQueryMetrics(query, queryToolChest);
                queryMetrics.reportCpuTime(cpuTimeNs).emit(emitter);
              }
            }
          }
        },
        Execs.newDirectExecutorService()
    );
  }

  public static <T> QueryRunner<T> safeBuild(
      QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      ServiceEmitter emitter,
      AtomicLong accumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      return delegate;
    } else {
      return new CPUTimeMetricQueryRunner<>(delegate, queryToolChest, emitter, accumulator, report);
    }
  }
}
