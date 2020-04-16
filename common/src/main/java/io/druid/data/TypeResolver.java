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

package io.druid.data;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;

import java.util.Map;

/**
 */
public interface TypeResolver
{
  ValueDesc resolve(String column);

  default ValueDesc resolve(String column, ValueDesc defaultType)
  {
    return Optional.fromNullable(resolve(column)).or(defaultType);
  }

  interface Resolvable
  {
    ValueDesc resolve(TypeResolver bindings);
  }

  interface LazyResolvable
  {
    ValueDesc resolve(Supplier<? extends TypeResolver> bindings);
  }

  class WithMap implements TypeResolver
  {
    final Map<String, ValueDesc> mapping;

    public WithMap(Map<String, ValueDesc> mapping) {this.mapping = mapping;}

    @Override
    public ValueDesc resolve(String column)
    {
      return mapping.get(column);
    }
  }

  class Delegate implements TypeResolver
  {
    private final TypeResolver delegated;

    public Delegate(TypeResolver delegated) {this.delegated = delegated;}

    @Override
    public ValueDesc resolve(String column)
    {
      return delegated.resolve(column);
    }

    @Override
    public ValueDesc resolve(String column, ValueDesc defaultType)
    {
      return delegated.resolve(column, defaultType);
    }
  }

  static Overriding override(TypeResolver delegated, Map<String, ValueDesc> overrides)
  {
    return override(delegated, new WithMap(overrides));
  }

  static Overriding override(TypeResolver delegated, TypeResolver overrides)
  {
    return new Overriding(delegated, overrides);
  }

  class Overriding extends Delegate
  {
    private final TypeResolver overrides;

    private Overriding(TypeResolver delegated, TypeResolver overrides)
    {
      super(delegated);
      this.overrides = overrides;
    }

    @Override
    public ValueDesc resolve(String column)
    {
      final ValueDesc resolved = overrides.resolve(column);
      return resolved != null ? resolved : super.resolve(column);
    }

    @Override
    public ValueDesc resolve(String column, ValueDesc defaultType)
    {
      final ValueDesc resolved = overrides.resolve(column);
      return resolved != null ? resolved : super.resolve(column, defaultType);
    }
  }

  class Updatable extends WithMap
  {
    public Updatable(Map<String, ValueDesc> mapping)
    {
      super(mapping);
    }

    public void putIfAbsent(String column, ValueDesc type)
    {
      mapping.putIfAbsent(column, type);
    }

    public ValueDesc remove(String column)
    {
      return mapping.remove(column);
    }
  }

  TypeResolver UNKNOWN = new TypeResolver()
  {
    @Override
    public ValueDesc resolve(String column)
    {
      return ValueDesc.UNKNOWN;
    }
  };

  TypeResolver STRING = new TypeResolver()
  {
    @Override
    public ValueDesc resolve(String column)
    {
      return ValueDesc.STRING;
    }
  };
}
