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
import io.druid.common.Yielders;
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 */
public class BaseSequence<T> implements Sequence<T>
{
  private static final Logger log = new Logger(BaseSequence.class);

  private final List<String> columns;
  private final IteratorMaker<T> maker;

  public BaseSequence(IteratorMaker<T> maker)
  {
    this(null, maker);
  }

  public BaseSequence(List<String> columns, IteratorMaker<T> maker)
  {
    this.columns = columns;
    this.maker = maker;
  }

  @Override
  public List<String> columns()
  {
    return columns;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, final Accumulator<OutType, T> fn)
  {
    Iterator<T> iterator = maker.make();
    try {
      while (iterator.hasNext()) {
        initValue = fn.accumulate(initValue, iterator.next());
      }
      return initValue;
    }
    finally {
      maker.cleanup(iterator);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final Iterator<T> iterator = maker.make();

    try {
      return makeYielder(initValue, accumulator, iterator);
    }
    catch (Exception e) {
      // We caught an Exception instead of returning a really, real, live, real boy, errr, iterator
      // So we better try to close our stuff, 'cause the exception is what is making it out of here.
      try {
        maker.cleanup(iterator);
      }
      catch (RuntimeException e1) {
        log.error(e1, "Exception thrown when closing maker.  Logging and ignoring.");
      }
      throw Throwables.propagate(e);
    }
  }

  private <OutType> Yielder<OutType> makeYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, T> accumulator,
      final Iterator<T> iter
  )
  {
    OutType retVal = initValue;
    while (!accumulator.yielded() && iter.hasNext()) {
      retVal = accumulator.accumulate(retVal, iter.next());
    }

    if (!accumulator.yielded()) {
      return Yielders.done(
          retVal,
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              maker.cleanup(iter);
            }
          }
      );
    }

    final OutType finalRetVal = retVal;
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return finalRetVal;
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        accumulator.reset();
        try {
          return makeYielder(initValue, accumulator, iter);
        }
        catch (Exception e) {
          // We caught an Exception instead of returning a really, real, live, real boy, errr, iterator
          // So we better try to close our stuff, 'cause the exception is what is making it out of here.
          try {
            maker.cleanup(iter);
          }
          catch (RuntimeException e1) {
            log.error(e1, "Exception thrown when closing maker.  Logging and ignoring.");
          }
          throw Throwables.propagate(e);
        }
      }

      @Override
      public boolean isDone()
      {
        return false;
      }

      @Override
      public void close() throws IOException
      {
        maker.cleanup(iter);
      }
    };
  }

  public static interface IteratorMaker<T>
  {
    Iterator<T> make();

    default void cleanup(Iterator<T> iterFromMake) {}
  }
}
