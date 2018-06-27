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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Accumulators;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.DelegatingYieldingAccumulator;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.common.Progressing;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 */
public class Sequences extends com.metamx.common.guava.Sequences
{
  public static <T> Sequence<T> mergeSort(Ordering<T> ordering, Sequence<Sequence<T>> baseSequences)
  {
    return new MergeSequence<T>(ordering, baseSequences);
  }

  public static <T> List<T> toList(Sequence<T> seq)
  {
    return seq.accumulate(Lists.<T>newArrayList(), Accumulators.<List<T>, T>list());
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

  @SuppressWarnings("unchecked")
  public static <T> Sequences.WithProgress<T> toSequence(
      final RowReader reader,
      final Function<Object, T> parser
  )
  {
    return new Sequences.WithProgress<T>()
    {
      @Override
      public float progress()
      {
        return reader.progress();
      }

      @Override
      public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
      {
        try {
          Object line;
          while ((line = reader.readRow()) != null) {
            initValue = accumulator.accumulate(initValue, parser.apply(line));
          }
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        finally {
          CloseQuietly.close(reader);
        }
        return initValue;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        CloseQuietly.close(reader);
        throw new UnsupportedOperationException("toYielder");
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

  public static final RowReader NULL_READER = new RowReader()
  {
    @Override
    public Object readRow() throws IOException, InterruptedException
    {
      return null;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public float progress()
    {
      return -1;
    }
  };

  public static interface RowReader extends Closeable, Progressing
  {
    Object readRow() throws IOException, InterruptedException;
  }

  public static interface WithProgress<T> extends Sequence<T>, Progressing
  {
  }
}
