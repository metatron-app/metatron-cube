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

import com.google.common.base.Predicate;

/**
*/
public class FilteringYieldingAccumulator<OutType, T> extends YieldingAccumulator<OutType, T>
{
  private final Predicate<T> pred;
  private final YieldingAccumulator<OutType, T> accumulator;

  private volatile boolean didSomething = false;

  public FilteringYieldingAccumulator(
      Predicate<T> pred,
      YieldingAccumulator<OutType, T> accumulator
  ) {
    this.pred = pred;
    this.accumulator = accumulator;
  }

  @Override
  public void yield()
  {
    accumulator.yield();
  }

  @Override
  public boolean yielded()
  {
    return accumulator.yielded();
  }

  @Override
  public void reset()
  {
    didSomething = false;
    accumulator.reset();
  }

  public boolean didSomething()
  {
    return didSomething;
  }

  @Override
  public OutType accumulate(OutType accumulated, T in)
  {
    if (pred.apply(in)) {
      if (!didSomething) {
        didSomething = true;
      }
      return accumulator.accumulate(accumulated, in);
    }
    return accumulated;
  }
}
