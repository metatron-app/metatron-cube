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

import java.io.Closeable;
import java.util.List;

/**
 */
public class ResourceClosingSequence<T> implements Sequence<T>
{
  private final Sequence<T> sequence;
  private final Closeable closeable;

  public ResourceClosingSequence(Sequence<T> sequence, Closeable closeable)
  {
    this.sequence = sequence;
    this.closeable = closeable;
  }

  @Override
  public List<String> columns()
  {
    return sequence.columns();
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    try {
      return sequence.accumulate(initValue, accumulator);
    }
    finally {
      CloseQuietly.close(closeable);
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      OutType initValue, YieldingAccumulator<OutType, T> accumulator
  )
  {
    final Yielder<OutType> baseYielder;
    try {
      baseYielder = sequence.toYielder(initValue, accumulator);
    }
    catch (Throwable e) {
      CloseQuietly.close(closeable);
      throw Throwables.propagate(e);
    }

    return new ResourceClosingYielder<>(baseYielder, closeable);
  }
}
