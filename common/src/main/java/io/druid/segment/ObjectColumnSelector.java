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

import java.util.List;
import java.util.function.IntFunction;

public interface ObjectColumnSelector<T> extends DSuppliers.TypedSupplier<T>
{
  interface Scannable<T> extends ObjectColumnSelector<T>, io.druid.common.Scannable<T>
  {
  }

  interface WithRawAccess<T> extends ObjectColumnSelector<T>, DSuppliers.WithRawAccess<T>
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

  public static <T> Typed<T> typed(ValueDesc type, Supplier<T> supplier)
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

  public static <T> Typed<T> typed(String description, ValueDesc type, Supplier<T> supplier)
  {
    return new Typed<T>(type)
    {
      @Override
      public T get()
      {
        return supplier.get();
      }

      @Override
      public String toString()
      {
        return description;
      }
    };
  }

  abstract class StringType extends Typed<String>
  {
    protected StringType()
    {
      super(ValueDesc.STRING);
    }
  }

  public static <T> Typed<T> string(Supplier<T> supplier)
  {
    return typed(ValueDesc.STRING, supplier);
  }

  interface ListBacked extends ObjectColumnSelector<List>
  {
    Object get(int index);
  }

  public static ObjectColumnSelector listBacked(ValueDesc type, IntFunction indexed, Supplier<List> whole)
  {
    return new ObjectColumnSelector.ListBacked()
    {
      @Override
      public ValueDesc type()
      {
        return type;
      }

      @Override
      public Object get(int index)
      {
        return indexed.apply(index);
      }

      @Override
      public List get()
      {
        return whole.get();
      }
    };
  }
}
