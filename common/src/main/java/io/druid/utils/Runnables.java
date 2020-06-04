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

import java.util.concurrent.Callable;

/**
 */
public class Runnables
{
  public static Runnable getNoopRunnable(){
    return new Runnable(){
      public void run(){}
    };
  }

  public static Runnable andThen(final Runnable run, final Runnable after)
  {
    return () -> { run.run(); after.run(); };
  }

  public static <T> Callable<T> before(final Callable<T> run, final Runnable before)
  {
    return () -> { before.run(); return run.call(); };
  }

  public static <T> Callable<T> after(final Callable<T> run, final Runnable after)
  {
    return () -> { T result = run.call(); after.run(); return result; };
  }
}
