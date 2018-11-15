/*
 * Licensed to Metaqualitys Group Inc. (Metaqualitys) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metaqualitys licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Accumulators;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.DelegatingYieldingAccumulator;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.parsers.CloseableIterator;
import io.druid.common.InterruptibleSequence;
import io.druid.common.Progressing;
import io.druid.common.Yielders;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 */
public class Sequences extends com.metamx.common.guava.Sequences
{
  public static <T> Sequence<T> lazy(Supplier<Sequence<T>> supplier)
  {
    return new LazySequence<>(supplier);
  }

  public static <T> Sequence<T> lazy(final Callable<Sequence<T>> callable)
  {
    return Sequences.<T>callableToLazy().apply(callable);
  }

  public static <T> Function<Callable<Sequence<T>>, Sequence<T>> callableToLazy()
  {
    return new Function<Callable<Sequence<T>>, Sequence<T>>()
    {
      @Override
      public Sequence<T> apply(final Callable<Sequence<T>> callable)
      {
        return new LazySequence<>(new Supplier<Sequence<T>>()
        {
          @Override
          public Sequence<T> get()
          {
            try {
              return callable.call();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        });
      }
    };
  }

  public static <T> Sequence<T> mergeSort(Ordering<T> ordering, Sequence<Sequence<T>> baseSequences)
  {
    return new MergeSequence<T>(ordering, baseSequences);
  }

  @SafeVarargs
  public static <T> Sequence<T> of(T... elements)
  {
    return BaseSequence.simple(Arrays.asList(elements));
  }

  public static <T> List<T> toList(Sequence<T> seq)
  {
    return seq.accumulate(Lists.<T>newArrayList(), Accumulators.<List<T>, T>list());
  }

  // todo : limit on concat is not working.. fuck
  public static <T> Sequence<T> concat(List<Sequence<T>> sequences)
  {
    return sequences.isEmpty() ? Sequences.<T>empty()
           : sequences.size() == 1 ? sequences.get(0) : concat(simple(sequences));
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

  public static <T> Sequence<T> once(final Iterator<T> iterator)
  {
    final Sequence<T> sequence = new BaseSequence<>(
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
      return new WithProgress<T>()
      {
        @Override
        public float progress()
        {
          return ((Progressing) iterator).progress();
        }

        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          return sequence.accumulate(initValue, accumulator);
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(
            OutType initValue, YieldingAccumulator<OutType, T> accumulator
        )
        {
          return sequence.toYielder(initValue, accumulator);
        }
      };
    }
    return sequence;
  }

  public static <T> Sequence<T> interruptible(Future<?> future, Sequence<T> sequence)
  {
    return new InterruptibleSequence<>(future, sequence);
  }

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
        return !yielder.isDone();
      }

      @Override
      public T next()
      {
        final T yield = yielder.get();
        yielder = yielder.next(null);
        return yield;
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

  public static interface WithProgress<T> extends Sequence<T>, Progressing
  {
  }
}
