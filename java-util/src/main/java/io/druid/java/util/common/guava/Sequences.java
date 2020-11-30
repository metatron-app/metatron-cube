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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class Sequences
{
  private static final EmptySequence EMPTY_SEQUENCE = new EmptySequence();

  public static <T> Sequence<T> simple(final Iterable<T> iterable)
  {
    return simple(null, iterable);
  }

  public static <T> Sequence<T> simple(final List<String> columns, final Iterable<T> iterable)
  {
    return new BaseSequence<>(columns, new BaseSequence.IteratorMaker<T, Iterator<T>>()
    {
      @Override
      public Iterator<T> make()
      {
        return iterable.iterator();
      }

      @Override
      public void cleanup(Iterator<T> iterator)
      {
        try {
          ((Closeable) iterator).close();
        }
        catch (Exception e) {
          // ignore
        }
      }
    });
  }

  @SafeVarargs
  public static <T> Sequence<T> of(T... elements)
  {
    return simple(Arrays.asList(elements));
  }

  @SafeVarargs
  public static <T> Sequence<T> of(List<String> columns, T... elements)
  {
    return simple(columns, Arrays.asList(elements));
  }

  @SuppressWarnings("unchecked")
  public static <T> Sequence<T> empty()
  {
    return (Sequence<T>) EMPTY_SEQUENCE;
  }

  public static <T> Sequence<T> empty(final List<String> columns)
  {
    return new EmptySequence<T>()
    {
      @Override
      public List<String> columns() { return columns;}
    };
  }

  @SafeVarargs
  public static <T> Sequence<T> concat(Sequence<T>... sequences)
  {
    return sequences.length == 0 ? empty() : concat(sequences[0].columns(), Sequences.of(sequences));
  }

  public static <T> Sequence<T> concat(Iterable<Sequence<T>> sequences)
  {
    return concat(null, Sequences.simple(sequences));
  }

  public static <T> Sequence<T> concat(List<String> columns, Iterable<Sequence<T>> sequences)
  {
    return concat(columns, Sequences.simple(columns, sequences));
  }

  public static <T> Sequence<T> concat(Sequence<Sequence<T>> sequences)
  {
    return concat(null, sequences);
  }

  public static <T> Sequence<T> concat(List<String> columns, Sequence<Sequence<T>> sequences)
  {
    return new ConcatSequence<T>(columns, sequences);
  }

  public static <From, To> Sequence<To> map(Sequence<From> sequence, Function<From, To> fn)
  {
    return MappedSequence.of(null, sequence, fn);
  }

  public static <T> Sequence<T> filter(Sequence<T> sequence, Predicate<T> pred)
  {
    return new FilteredSequence<>(sequence, pred);
  }

  public static <T> Sequence<T> limit(Sequence<T> sequence, final int limit)
  {
    return new LimitedSequence<>(sequence, limit);
  }

  public static <T> Sequence<T> withBaggage(Sequence<T> sequence, Closeable baggage)
  {
    return new ResourceClosingSequence<>(sequence, baggage);
  }

  public static <T> List<T> toList(Sequence<T> sequence)
  {
    return sequence.accumulate(Lists.<T>newArrayList(), Accumulators.<List<T>, T>list());
  }

  public static <T, ListType extends List<T>> ListType toList(Sequence<T> sequence, ListType list)
  {
    return sequence.accumulate(list, Accumulators.<ListType, T>list());
  }

  public static <T> Sequence<T> materialize(Sequence<T> sequence)
  {
    return Sequences.simple(sequence.columns(), Sequences.toList(sequence));
  }

  private static class EmptySequence<T> implements Sequence<T>
  {
    @Override
    public List<String> columns()
    {
      return ImmutableList.of();
    }

    @Override
    public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
    {
      return initValue;
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
    {
      return Yielders.done(initValue, null);
    }
  }
}
