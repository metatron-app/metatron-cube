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

package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.Accumulators;
import io.druid.common.Progressing;
import io.druid.common.Yielders;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.BaseSequence;
import io.druid.common.guava.ConcatSequence;
import io.druid.common.guava.DelegatingYieldingAccumulator;
import io.druid.common.guava.ExecuteWhenDoneYielder;
import io.druid.common.guava.FilteredSequence;
import io.druid.common.guava.FutureSequence;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.InterruptibleSequence;
import io.druid.common.guava.LazySequence;
import io.druid.common.guava.LimitedSequence;
import io.druid.common.guava.MappedSequence;
import io.druid.common.guava.MergeSequence;
import io.druid.common.guava.ParallelInitMergeSequence;
import io.druid.common.guava.ResourceClosingSequence;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.guava.YielderMergeSequence;
import io.druid.common.guava.YieldingAccumulator;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.parsers.CloseableIterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

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
    return new BaseSequence<>(columns, new BaseSequence.IteratorMaker<T>()
    {
      @Override
      public Iterator<T> make()
      {
        return iterable.iterator();
      }

      @Override
      public void cleanup(Iterator<T> iterator)
      {
        if (iterator instanceof Closeable) {
          IOUtils.closeQuietly((Closeable) iterator);
        }
      }
    });
  }

  @SafeVarargs
  public static <T> Sequence<T> of(T... elements)
  {
    return new Materialized<>(null, Arrays.asList(elements));
  }

  @SafeVarargs
  public static <T> Materialized<T> from(List<String> columns, T... elements)
  {
    return from(columns, Arrays.asList(elements));
  }

  public static <T> Materialized<T> from(List<String> columns, List<T> elements)
  {
    return new Materialized<T>(columns, elements);
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

  public static <T> int size(Sequence<T> sequence)
  {
    Accumulator<MutableInt, T> outTypeTAccumulator = (counter, row) -> {
      counter.increment();
      return counter;
    };
    return sequence.accumulate(new MutableInt(), outTypeTAccumulator).intValue();
  }

  public static <T> List<T> toList(Sequence<T> sequence)
  {
    if (sequence instanceof Materialized) {
      return Lists.newArrayList(((Materialized<T>) sequence).values);
    }
    return sequence.accumulate(Lists.<T>newArrayList(), Accumulators.<List<T>, T>list());
  }

  public static <T, ListType extends List<T>> ListType toList(Sequence<T> sequence, ListType list)
  {
    return sequence.accumulate(list, Accumulators.<ListType, T>list());
  }

  public static <T> Materialized<T> materialize(Sequence<T> sequence)
  {
    return new Materialized<T>(sequence.columns(), Sequences.toList(sequence));
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
  @SafeVarargs
  public static <T> Sequence<T> simple(final T a, T... as)
  {
    return simple(GuavaUtils.concat(a, Arrays.asList(as)));
  }

  public static <T> T accumulate(final Sequence<T> sequence, final BinaryFn<T, T, T> function)
  {
    return sequence.accumulate(null, new Accumulator<T, T>()
    {
      @Override
      public T accumulate(T accumulated, T in)
      {
        return function.apply(accumulated, in);
      }
    });
  }

  public static <T> Sequence<T> lazy(Supplier<Sequence<T>> supplier)
  {
    return lazy(null, supplier);
  }

  public static <T> Sequence<T> lazy(List<String> columns, Supplier<Sequence<T>> supplier)
  {
    return new LazySequence<>(columns, supplier);
  }

  public static <T> Sequence<T> future(Future<Sequence<T>> supplier)
  {
    return future(null, supplier);
  }

  public static <T> Sequence<T> future(List<String> columns, Future<Sequence<T>> supplier)
  {
    return new FutureSequence<>(columns, supplier);
  }

  public static <T> List<Yielder<T>> toYielders(Sequence<Sequence<T>> sequences)
  {
    return sequences.accumulate(
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
  }

  public static <T> Sequence<T> mergeSort(List<String> columns, Comparator<T> ordering, Sequence<Sequence<T>> sequences)
  {
    return new MergeSequence<T>(columns, ordering, sequences);
  }

  public static <T> Sequence<T> mergeSort(List<String> columns, Comparator<T> ordering, List<Yielder<T>> yielders)
  {
    return new YielderMergeSequence<T>(columns, ordering, Yielders.prepare(yielders));
  }

  public static <T> Sequence<T> mergeSort(
      List<String> columns,
      Comparator<T> ordering,
      Sequence<Sequence<T>> baseSequences,
      ExecutorService executor
  )
  {
    return new ParallelInitMergeSequence<T>(ordering, columns, baseSequences, executor);
  }

  public static <T> Sequence<T> filterNull(Sequence<T> sequence)
  {
    return filter(sequence, Predicates.<T>notNull());
  }

  public static <T> T only(Sequence<T> seq)
  {
    return Iterables.getOnlyElement(toList(seq));
  }

  public static <T> T only(Sequence<T> seq, T defaultValue)
  {
    return Iterables.getOnlyElement(toList(seq), defaultValue);
  }

  public static <T> Sequence<T> concat(List<Sequence<T>> sequences)
  {
    return sequences.isEmpty() ? Sequences.<T>empty() :
           sequences.size() == 1 ? sequences.get(0) :
           concat(sequences.get(0).columns(), simple(sequences));
  }

  public static Sequence<Object[]> repeat(Object[] v, int exploded)
  {
    if (exploded == 1) {
      return Sequences.simple(v);
    }
    return Sequences.once(new Iterator<Object[]>()
    {
      private int index;

      @Override
      public boolean hasNext()
      {
        return index < exploded;
      }

      @Override
      public Object[] next()
      {
        return index++ == 0 ? v : Arrays.copyOf(v, v.length);
      }
    });
  }

  public static <From, To> Sequence<To> explode(Sequence<From> sequence, Function<From, Sequence<To>> fn)
  {
    return concat(sequence.columns(), map(sequence, fn));
  }

  public static <From, To> Sequence<To> explode(
      List<String> columns,
      Sequence<From> sequence,
      Function<From, Sequence<To>> fn
  )
  {
    return concat(columns, map(sequence, fn));
  }

  public static Sequence<Object[]> projection(Sequence<Object[]> sequence, int[] mapping)
  {
    final List<String> output = projection(sequence.columns(), mapping);
    return map(output, sequence, new Function<Object[], Object[]>()
    {
      @Override
      public Object[] apply(Object[] input)
      {
        final Object[] output = new Object[mapping.length];
        for (int i = 0; i < mapping.length; i++) {
          if (mapping[i] >= 0) {
            output[i] = input[mapping[i]];
          }
        }
        return output;
      }
    });
  }

  private static List<String> projection(List<String> columns, int[] mapping)
  {
    if (columns != null) {
      List<String> reordered = Lists.newArrayList();
      for (int x : mapping) {
        reordered.add(GuavaUtils.get(columns, x));
      }
      return reordered;
    }
    return columns;
  }

  public static <F, T> Function<Sequence<F>, Sequence<T>> mapper(List<String> columns, Function<F, T> f)
  {
    return input -> Sequences.map(columns, input, f);
  }

  public static <T> Sequence<T> withEffect(Sequence<T> sequence, Runnable effect)
  {
    return withEffect(sequence, effect, Execs.newDirectExecutorService());
  }

  public static <T> Sequence<T> withEffect(final Sequence<T> sequence, final Runnable effect, final Executor exec)
  {
    return new Sequence<T>()
    {
      @Override
      public List<String> columns()
      {
        return sequence.columns();
      }

      @Override
      public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
      {
        final OutType out = sequence.accumulate(initValue, accumulator);
        exec.execute(effect);
        return out;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        return new ExecuteWhenDoneYielder<>(sequence.toYielder(initValue, accumulator), effect, exec);
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static <T> T[] toArray(Sequence<T> seq, Class<T> clazz)
  {
    return toList(seq).toArray((T[]) Array.newInstance(clazz, 0));
  }

  public static <T> Function<Iterable<T>, Sequence<T>> toSequence()
  {
    return new Function<Iterable<T>, Sequence<T>>()
    {
      @Override
      public Sequence<T> apply(Iterable<T> input)
      {
        return Sequences.simple(input);
      }
    };
  }

  public static <From, To> Sequence<To> map(Sequence<From> sequence, Function<From, To> fn)
  {
    return map(null, sequence, fn);
  }

  public static <T> Sequence<T> peek(Sequence<T> sequence, Consumer<T> fn)
  {
    return map(null, sequence, v -> {fn.accept(v);return v;});
  }

  public static <From, To> Sequence<To> map(List<String> columns, Sequence<From> sequence, Function<From, To> fn)
  {
    Sequence<To> mapped = MappedSequence.of(columns, sequence, fn);
    if (sequence instanceof Progressing) {
      mapped = new ProgressingSequence<>(mapped, (Progressing) sequence);
    }
    return mapped;
  }

  public static <T> Iterator<T> concat(final Iterator<Iterator<T>> readers)
  {
    return new Progressing.OnIterator<T>()
    {
      private Iterator<T> current = Collections.emptyIterator();

      @Override
      public float progress()
      {
        return current instanceof Progressing ? ((Progressing) current).progress() : hasNext() ? 0 : 1;
      }

      @Override
      public void close() throws IOException
      {
        if (current instanceof Closeable) {
          ((Closeable) current).close();
        }
      }

      @Override
      public boolean hasNext()
      {
        for (; !current.hasNext() && readers.hasNext(); current = closeAndNext()) {
        }
        return current.hasNext();
      }

      public Iterator<T> closeAndNext()
      {
        try {
          close();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return readers.next();
      }

      @Override
      public T next()
      {
        return current.next();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public static <T> Sequence<T> once(final Iterator<T> iterator)
  {
    return once(null, iterator);
  }

  public static <T> Sequence<T> once(final List<String> columns, final Iterator<T> iterator)
  {
    Sequence<T> sequence = new BaseSequence<>(
        columns,
        new BaseSequence.IteratorMaker<T>()
        {
          @Override
          public Iterator<T> make()
          {
            return iterator;
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            if (iterator instanceof Closeable) {
              IOUtils.closeQuietly((Closeable) iterator);
            }
          }
        }
    );
    if (iterator instanceof Progressing) {
      sequence = new ProgressingSequence<T>(sequence, (Progressing) iterator);
    }
    return sequence;
  }

  public static <T> Sequence<T> interruptible(Future<?> future, Sequence<T> sequence)
  {
    return new InterruptibleSequence<>(future, sequence);
  }

  // warn : use only for trivial case
  public static <T> CloseableIterator<T> toIterator(final Sequence<T> sequence)
  {
    return new CloseableIterator<T>()
    {
      private Yielder<T> yielder = Yielders.each(sequence);

      @Override
      public boolean hasNext()
      {
        if (yielder == null || yielder.isDone()) {
          IOUtils.closeQuietly(yielder);
          yielder = null;
          return false;
        }
        return true;
      }

      @Override
      public T next()
      {
        try {
          final T yield = yielder.get();
          yielder = yielder.next(null);
          return yield;
        }
        catch (Exception e) {
          IOUtils.closeQuietly(yielder);
          yielder = null;
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        if (yielder != null) {
          yielder.close();
          yielder = null;
        }
      }
    };
  }

  public abstract static class PeekingSequence<T> implements Sequence<T>
  {
    private final Sequence<T> sequence;

    protected PeekingSequence(Sequence<T> sequence) {this.sequence = sequence;}

    @Override
    public List<String> columns()
    {
      return sequence.columns();
    }

    @Override
    public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> accumulator)
    {
      return sequence.accumulate(
          initValue, new Accumulator<OutType, T>()
          {
            @Override
            public OutType accumulate(OutType accumulated, T in)
            {
              return accumulator.accumulate(accumulated, peek(in));
            }
          }
      );
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(
        OutType initValue, YieldingAccumulator<OutType, T> accumulator
    )
    {
      return sequence.toYielder(
          initValue, new DelegatingYieldingAccumulator<OutType, T>(accumulator)
          {
            @Override
            public OutType accumulate(OutType accumulated, T in)
            {
              return super.accumulate(accumulated, peek(in));
            }
          }
      );
    }

    protected abstract T peek(T row);
  }

  public static class ProgressingSequence<T> extends Sequence.Delegate<T> implements Progressing
  {
    private final Progressing progressing;

    public ProgressingSequence(Sequence<T> sequence, Progressing progressing)
    {
      super(sequence);
      this.progressing = progressing;
    }

    @Override
    public float progress()
    {
      return progressing.progress();
    }

    @Override
    public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
    {
      return sequence.accumulate(initValue, accumulator);
    }

    @Override
    public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
    {
      return sequence.toYielder(initValue, accumulator);
    }
  }

  private static class Materialized<T> extends BaseSequence<T>
  {
    private final List<T> values;

    public Materialized(List<String> columns, List<T> values)
    {
      super(columns, () -> values.iterator());
      this.values = values;
    }
  }
}
