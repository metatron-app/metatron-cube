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

package io.druid.common.guava;

import com.google.common.base.Supplier;
import io.druid.data.ValueDesc;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DSuppliers
{
  public static interface Typed
  {
    ValueDesc type();
  }

  public static interface TypedSupplier<T> extends Supplier<T>, Typed
  {
    class Simple<T> implements TypedSupplier<T>
    {
      private final T value;
      private final ValueDesc type;

      public Simple(T value, ValueDesc type)
      {
        this.value = value;
        this.type = type;
      }

      @Override
      public T get()
      {
        return value;
      }

      @Override
      public ValueDesc type()
      {
        return type;
      }
    }

    Simple<Object> UNKNOWN = new Simple<Object>(null, ValueDesc.UNKNOWN);
  }

  public static interface WithRawAccess<T> extends TypedSupplier<T>
  {
    byte[] getRaw();

    BufferRef getAsRef();
  }

  public static <T> Supplier<T> of(final AtomicReference<T> ref)
  {
    return new Supplier<T>()
    {
      @Override
      public T get()
      {
        return ref.get();
      }
    };
  }

  public static class HandOver<T> implements Supplier<T>
  {
    private transient volatile T object;

    @Override
    public T get()
    {
      return object;
    }

    public void set(T object)
    {
      this.object = object;
    }
  }

  public static class ThreadSafe<T> implements Supplier<T>, Closeable
  {
    private final ThreadLocal<T> threadLocal = new ThreadLocal<>();

    @Override
    public T get()
    {
      return threadLocal.get();
    }

    public void set(T object)
    {
      threadLocal.set(object);
    }

    @Override
    public void close() throws IOException
    {
      threadLocal.remove();
    }
  }

  public static Object lazyLog(Supplier supplier)
  {
    return new Object()
    {
      @Override
      public String toString()
      {
        return Objects.toString(supplier.get(), null);
      }
    };
  }
}
