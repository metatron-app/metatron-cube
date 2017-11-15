package io.druid.common.utils;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.DelegatingYieldingAccumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.common.Progressing;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class Sequences extends com.metamx.common.guava.Sequences
{
  @SuppressWarnings("unchecked")
  public static <T> Sequences.WithProgress<T> toSequence(
      final RowReader reader,
      final Function<Object, T> parser
  )
  {
    return new Sequences.WithProgress<T>()
    {
      @Override
      public float progress() throws IOException, InterruptedException
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
    public float progress() throws IOException, InterruptedException
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
