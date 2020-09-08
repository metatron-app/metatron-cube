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

package io.druid.java.util.common.guava;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;

/**
 */
public class MergeSequence<T> extends BaseMergeSequence<T>
{
  private final Comparator<T> ordering;
  private final Sequence<Sequence<T>> sequences;

  public MergeSequence(Comparator<T> ordering, Sequence<Sequence<T>> sequences)
  {
    this.ordering = ordering;
    this.sequences = sequences;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final List<Yielder<T>> yielders = sequences.accumulate(
        Lists.newArrayList(), new Accumulator<List<Yielder<T>>, Sequence<T>>()
        {
          @Override
          public List<Yielder<T>> accumulate(List<Yielder<T>> yielders, Sequence<T> sequence)
          {
            try {
              final Yielder<T> yielder = Yielders.each(sequence);
              if (yielder.isDone()) {
                Yielders.close(yielder);
              } else {
                yielders.add(yielder);
              }
              return yielders;
            }
            catch (Exception e) {
              for (Yielder<T> yielder : yielders) {
                Yielders.close(yielder);
              }
              yielders.clear();
              throw Throwables.propagate(e);
            }
          }
        }
    );
    return makeYielder(createQueue(ordering, yielders), initValue, accumulator);
  }
}
