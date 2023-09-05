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
import com.google.common.base.Throwables;
import io.druid.data.ValueDesc;
import io.druid.segment.Tools;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
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
    abstract class FixedTyped<T> implements TypedSupplier<T>
    {
      private final ValueDesc type;

      public FixedTyped(ValueDesc type) {this.type = type;}

      @Override
      public final ValueDesc type()
      {
        return type;
      }
    }

    class Simple<T> extends FixedTyped<T>
    {
      private final T value;

      public Simple(T value, ValueDesc type)
      {
        super(type);
        this.value = value;
      }

      @Override
      public T get()
      {
        return value;
      }
    }

    Simple<Object> UNKNOWN = new Simple<Object>(null, ValueDesc.UNKNOWN);
  }

  public static <T> TypedSupplier<T> asTypedSupplier(ValueDesc type, Supplier<T> supplier)
  {
    return new TypedSupplier.FixedTyped<T>(type)
    {
      @Override
      public T get()
      {
        return supplier.get();
      }
    };
  }

  public static interface WithRawAccess<T> extends TypedSupplier<T>
  {
    byte[] getAsRaw();

    BufferRef getAsRef();

    <R> R apply(Tools.Function<R> function);
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

    public T setAndReturn(T object)
    {
      this.object = object;
      return object;
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

  public static <T> Supplier<T> wrap(Callable<T> callable)
  {
    return new Supplier<T>()
    {
      @Override
      public T get()
      {
        try {
          return callable.call();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  public static <T> Memoizing<T> memoize(Callable<T> callable)
  {
    return new Memoizing<>(wrap(callable));
  }

  // com.google.common.base.Suppliers
  public static class Memoizing<T> implements Supplier<T>
  {
    private final Supplier<T> delegate;
    private volatile long initialized = -1;
    private T value;

    Memoizing(Supplier<T> delegate)
    {
      this.delegate = delegate;
    }

    public synchronized void reset()
    {
      initialized = -1;
      value = null;
    }

    public synchronized long updated()
    {
      return initialized;
    }

    public boolean initialized()
    {
      if (initialized > 0) {
        return true;
      }
      synchronized (this) {
        return initialized > 0;
      }
    }

    @Override
    public T get()
    {
      // A 2-field variant of Double-Checked Locking.
      if (initialized < 0) {
        synchronized (this) {
          if (initialized < 0) {
            T t = delegate.get();
            value = t;
            initialized = System.currentTimeMillis();
            return t;
          }
        }
      }
      return value;
    }
  }
}
