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

package io.druid.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StopWatch
{
  private final long timeout;

  public StopWatch(long timeout)
  {
    this.timeout = System.currentTimeMillis() + timeout;
  }

  public <T> T wainOn(Future<T> future) throws TimeoutException, ExecutionException, InterruptedException
  {
    final long remaining = remaining();
    if (remaining <= 0) {
      throw new TimeoutException();
    }
    return future.get(remaining, TimeUnit.MILLISECONDS);
  }

  public <T> boolean enqueue(BlockingQueue<T> queue, T element) throws TimeoutException, InterruptedException
  {
    for (long remaining = remaining(); remaining > 0; remaining = remaining()) {
      if (queue.offer(element, remaining, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }
    throw new TimeoutException();
  }

  public <T> T dequeue(BlockingQueue<T> queue) throws TimeoutException, InterruptedException
  {
    for (long remaining = remaining(); remaining > 0; remaining = remaining()) {
      T poll = queue.poll(remaining, TimeUnit.MILLISECONDS);
      if (poll != null) {
        return poll;
      }
    }
    throw new TimeoutException();
  }

  public boolean acquire(Semaphore semaphore) throws TimeoutException, InterruptedException
  {
    for (long remaining = remaining(); remaining > 0; remaining = remaining()) {
      if (semaphore.tryAcquire(remaining, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }
    throw new TimeoutException();
  }

  public long remaining()
  {
    return timeout - System.currentTimeMillis();
  }

  public boolean isExpired()
  {
    return remaining() <= 0;
  }
}
