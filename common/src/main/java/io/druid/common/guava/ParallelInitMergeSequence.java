/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.common.guava;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.BaseMergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.commons.io.IOUtils;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ParallelInitMergeSequence<T> extends BaseMergeSequence<T>
{
  private final Comparator<T> ordering;
  private final Sequence<Sequence<T>> sequences;
  private final ExecutorService executor;

  public ParallelInitMergeSequence(Comparator<T> ordering, Sequence<Sequence<T>> sequences, ExecutorService executor)
  {
    this.ordering = ordering;
    this.sequences = sequences;
    this.executor = executor;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final List<Future<Yielder<T>>> futures = sequences.accumulate(
        Lists.newArrayList(), new Accumulator<List<Future<Yielder<T>>>, Sequence<T>>()
        {
          @Override
          public List<Future<Yielder<T>>> accumulate(List<Future<Yielder<T>>> futures, Sequence<T> sequence)
          {
            futures.add(executor.submit(() -> Yielders.each(sequence)));
            return futures;
          }
        }
    );
    final List<Yielder<T>> yielders = Lists.newArrayList();
    for (Future<Yielder<T>> future : futures) {
      try {
        Yielder<T> yielder = future.get();
        if (yielder.isDone()) {
          executor.submit(() -> Yielders.close(yielder));
        } else {
          yielders.add(yielder);
        }
      }
      catch (Throwable t) {
        executor.submit(new Runnable()
        {
          @Override
          public void run()
          {
            for (Future<Yielder<T>> future : futures) {
              IOUtils.closeQuietly(() -> Futures.getUnchecked(future));
            }
          }
        });
        if (t instanceof ExecutionException && t.getCause() != null) {
          t = t.getCause();
        }
        throw Throwables.propagate(t);
      }
    }
    return makeYielder(createQueue(ordering, yielders), initValue, accumulator);
  }
}
