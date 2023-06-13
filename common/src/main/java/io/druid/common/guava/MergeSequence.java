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

import io.druid.common.utils.Sequences;

import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class MergeSequence<T> extends BaseMergeSequence<T>
{
  private final List<String> columns;
  private final Comparator<T> ordering;
  private final Sequence<Sequence<T>> sequences;

  public MergeSequence(
      List<String> columns,
      Comparator<T> ordering,
      Sequence<Sequence<T>> sequences
  )
  {
    this.ordering = ordering;
    this.columns = columns;
    this.sequences = sequences;
  }

  public MergeSequence(Comparator<T> ordering, Sequence<Sequence<T>> sequences)
  {
    this(null, ordering, sequences);
  }

  @Override
  public List<String> columns()
  {
    return columns;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return makeYielder(createQueue(ordering, Sequences.toYielders(sequences)), initValue, accumulator);
  }
}
