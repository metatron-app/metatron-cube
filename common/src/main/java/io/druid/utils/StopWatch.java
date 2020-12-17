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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
    final long remaining = timeout - System.currentTimeMillis();
    if (remaining <= 0) {
      throw new TimeoutException();
    }
    return future.get(remaining, TimeUnit.MILLISECONDS);
  }

  public boolean isExpired()
  {
    return timeout < System.currentTimeMillis();
  }
}
