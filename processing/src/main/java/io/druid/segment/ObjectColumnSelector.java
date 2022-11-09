/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.segment;

import com.google.common.base.Supplier;
import io.druid.common.guava.DSuppliers;
import io.druid.data.ValueDesc;

import java.io.Closeable;

public interface ObjectColumnSelector<T> extends DSuppliers.TypedSupplier<T>
{
  interface WithBaggage<T> extends ObjectColumnSelector<T>, Closeable
  {
  }

  interface WithRawAccess<T> extends WithBaggage<T>, DSuppliers.WithRawAccess<T>
  {
  }

  abstract class Typed<T> implements ObjectColumnSelector<T>
  {
    private final ValueDesc type;

    protected Typed(ValueDesc type) {this.type = type;}

    @Override
    public final ValueDesc type()
    {
      return type;
    }

  }

  public static <T> Typed<T> with(ValueDesc type, Supplier<T> supplier)
  {
    return new Typed<T>(type)
    {
      @Override
      public T get()
      {
        return supplier.get();
      }
    };
  }
}
