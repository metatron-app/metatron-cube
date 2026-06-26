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

package io.druid.java.util.common;

/**
 * Drop-in replacement for the {@code propagate*} methods removed from
 * Guava's {@code com.google.common.base.Throwables} (Guava 20+). Semantics
 * match the original Guava implementations so existing call sites are unchanged
 * apart from the import.
 */
public final class Throwables
{
  private Throwables()
  {
  }

  /**
   * Rethrows {@code throwable} if it is an {@link Error} or {@link RuntimeException},
   * otherwise wraps it in a {@link RuntimeException}. Never returns normally; the
   * {@link RuntimeException} return type lets callers write {@code throw propagate(t);}.
   */
  public static RuntimeException propagate(Throwable throwable)
  {
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
    throw new RuntimeException(throwable);
  }

  public static <X extends Throwable> void propagateIfInstanceOf(Throwable throwable, Class<X> declaredType) throws X
  {
    if (throwable != null && declaredType.isInstance(throwable)) {
      throw declaredType.cast(throwable);
    }
  }

  public static void propagateIfPossible(Throwable throwable)
  {
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
  }

  public static <X extends Throwable> void propagateIfPossible(Throwable throwable, Class<X> declaredType) throws X
  {
    propagateIfInstanceOf(throwable, declaredType);
    propagateIfPossible(throwable);
  }
}
