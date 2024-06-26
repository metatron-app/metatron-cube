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

import java.io.Closeable;
import java.io.IOException;

/**
 */
public class ResourceClosingYielder<OutType> implements Yielder<OutType>
{
  private final Yielder<OutType> baseYielder;
  private final Closeable closeable;

  public ResourceClosingYielder(Yielder<OutType> baseYielder, Closeable closeable)
  {
    this.baseYielder = baseYielder;
    this.closeable = closeable;
  }

  @Override
  public OutType get()
  {
    return baseYielder.get();
  }

  @Override
  public Yielder<OutType> next(OutType initValue)
  {
    return new ResourceClosingYielder<>(baseYielder.next(initValue), closeable);
  }

  @Override
  public boolean isDone()
  {
    return baseYielder.isDone();
  }

  @Override
  public void close() throws IOException
  {
    if (closeable != null) {
      closeable.close();
    }
    baseYielder.close();
  }
}
