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

import com.google.common.base.Function;

import java.util.List;

/**
 */
public class MappedSequence<T, Out> implements Sequence<Out>
{
  public static <T, OUT> MappedSequence<T, OUT> of(List<String> columns, Sequence<T> baseSequence, Function<T, OUT> fn)
  {
    return columns == null ? new MappedSequence<>(baseSequence, fn) : new Explicit<>(columns, baseSequence, fn);
  }

  private final Sequence<T> baseSequence;
  private final Function<T, Out> fn;

  private MappedSequence(Sequence<T> baseSequence, Function<T, Out> fn)
  {
    this.baseSequence = baseSequence;
    this.fn = fn;
  }

  @Override
  public List<String> columns()
  {
    return baseSequence.columns();
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, Out> accumulator)
  {
    return baseSequence.accumulate(initValue, new MappingAccumulator<>(fn, accumulator));
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, Out> accumulator)
  {
    return baseSequence.toYielder(initValue, new MappingYieldingAccumulator<>(fn, accumulator));
  }

  private static class Explicit<T, OUT> extends MappedSequence<T, OUT>
  {
    private final List<String> columns;

    private Explicit(List<String> columns, Sequence<T> baseSequence, Function<T, OUT> fn)
    {
      super(baseSequence, fn);
      this.columns = columns;
    }

    @Override
    public List<String> columns()
    {
      return columns;
    }
  }
}
