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

/**
 */
public class DelegatingYieldingAccumulator<OutType, T> extends YieldingAccumulator<OutType, T>
{
  private final YieldingAccumulator<OutType, T> delegate;

  public DelegatingYieldingAccumulator(
      YieldingAccumulator<OutType, T> delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public void yield()
  {
    delegate.yield();
  }

  @Override
  public boolean yielded()
  {
    return delegate.yielded();
  }

  @Override
  public void reset()
  {
    delegate.reset();
  }

  @Override
  public OutType accumulate(OutType accumulated, T in)
  {
    return delegate.accumulate(accumulated, in);
  }
}
