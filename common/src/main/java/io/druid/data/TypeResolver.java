/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data;

import com.google.common.base.Optional;

import java.util.Map;

/**
 */
public interface TypeResolver
{
  ValueDesc resolve(String column);

  ValueDesc resolve(String column, ValueDesc defaultType);

  interface Resolvable
  {
    ValueDesc resolve(TypeResolver bindings);
  }

  abstract class Abstract implements TypeResolver
  {
    @Override
    public ValueDesc resolve(String column, ValueDesc defaultType)
    {
      return Optional.fromNullable(resolve(column)).or(defaultType);
    }
  }

  class WithMap extends Abstract
  {
    private final Map<String, ValueDesc> mapping;

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
}
