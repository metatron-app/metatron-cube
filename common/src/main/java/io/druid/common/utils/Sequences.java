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
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.common.InterruptibleSequence;
import io.druid.common.Progressing;
import io.druid.common.Yielders;
import io.druid.common.guava.ExecuteWhenDoneYielder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.ParallelInitMergeSequence;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Accumulators;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.DelegatingYieldingAccumulator;
import io.druid.java.util.common.guava.LazySequence;
import io.druid.java.util.common.guava.MappedSequence;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.parsers.CloseableIterator;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class Sequences extends io.druid.java.util.common.guava.Sequences
{
  public static <T> Sequence<T> simple(final Iterable<T> iterable)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
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
        }
    );
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
    return new LazySequence<>(supplier);
  }

  public static <T> Sequence<T> mergeSort(Ordering<T> ordering, Iterable<Sequence<T>> baseSequences)
  {
    return mergeSort(ordering, Sequences.simple(baseSequences));
  }

  public static <T> Sequence<T> mergeSort(Ordering<T> ordering, Sequence<Sequence<T>> baseSequences)
  {
    return new MergeSequence<T>(ordering, baseSequences);
  }

  public static <T> Sequence<T> mergeSort(
      Ordering<T> ordering,
      Sequence<Sequence<T>> baseSequences,
      ExecutorService executor
  )
  {
    return new ParallelInitMergeSequence<T>(ordering, baseSequences, executor);
  }

  public static <T> Sequence<T> filterNull(Sequence<T> sequence)
  {
    return filter(sequence, Predicates.<T>notNull());
  }

  @SafeVarargs
  public static <T> Sequence<T> of(T... elements)
  {
    return BaseSequence.simple(Arrays.asList(elements));
  }

  public static <T> T only(Sequence<T> seq)
  {
    return Iterables.getOnlyElement(toList(seq));
  }

  public static <T> T only(Sequence<T> seq, T defaultValue)
  {
    return Iterables.getOnlyElement(toList(seq), defaultValue);
  }

  public static <T> List<T> toList(Sequence<T> seq)
  {
    return seq.accumulate(Lists.<T>newArrayList(), Accumulators.<List<T>, T>list());
  }

  public static <T> Sequence<T> concat(List<Sequence<T>> sequences)
  {
    return sequences.isEmpty() ? Sequences.<T>empty()
           : sequences.size() == 1 ? sequences.get(0) : concat(simple(sequences));
  }

  public static <From, To> Sequence<To> explode(Sequence<From> sequence, Function<From, Sequence<To>> fn)
  {
    return concat(map(sequence, fn));
  }

  public static <From, M, To> Sequence<To> map(Sequence<From> sequence, Function<From, M> fn1, Function<M, To> fn2)
  {
    return Sequences.map(Sequences.map(sequence, fn1), fn2);
  }

  public static <F, T> Function<Sequence<F>, Sequence<T>> mapper(Function<F, T> f)
  {
    return input -> Sequences.map(input, f);
  }

  public static <T> Sequence<T> withEffect(Sequence<T> sequence, Runnable effect)
  {
    return withEffect(sequence, effect, Execs.newDirectExecutorService());
  }

  public static <T> Sequence<T> withEffect(final Sequence <T> seq, final Runnable effect, final Executor exec)
  {
    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
      {
        final OutType out = seq.accumulate(initValue, accumulator);
        exec.execute(effect);
        return out;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        return new ExecuteWhenDoneYielder<>(seq.toYielder(initValue, accumulator), effect, exec);
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
    Sequence<To> mapped = new MappedSequence<>(sequence, fn);
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
    Sequence<T> sequence = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
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
      @Override
      public void close() throws IOException
      {
        yielder.close();
      }

      private Yielder<T> yielder = Yielders.each(sequence);

      @Override
      public boolean hasNext()
      {
        if (yielder.isDone()) {
          IOUtils.closeQuietly(yielder);
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
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public abstract static class PeekingSequence<T> implements Sequence<T>
  {
    private final Sequence<T> sequence;

    protected PeekingSequence(Sequence<T> sequence) {this.sequence = sequence;}

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

  public static class ProgressingSequence<T> implements Sequence<T>, Progressing
  {
    private final Progressing progressing;
    private final Sequence<T> sequence;

    public ProgressingSequence(Sequence<T> sequence, Progressing progressing)
    {
      this.progressing = progressing;
      this.sequence = sequence;
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
}
