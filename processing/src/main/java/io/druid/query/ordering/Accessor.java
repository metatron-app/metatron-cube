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

package io.druid.query.ordering;

import com.google.common.primitives.Longs;

import java.util.Comparator;

public interface Accessor<T>
{
  Object get(T source);

  class TimeComparator<T> implements Comparator<T>
  {
    private final Accessor<T> accessor;

    public TimeComparator(Accessor<T> accessor) {this.accessor = accessor;}

    @Override
    public int compare(T left, T right)
    {
      return Longs.compare((Long) accessor.get(left), (Long) accessor.get(right));
    }
  }

  class ComparatorOn<T> implements Comparator<T>
  {
    private final Comparator comparator;
    private final Accessor<T> accessor;

    public ComparatorOn(Comparator comparator, Accessor<T> accessor)
    {
      this.comparator = comparator;
      this.accessor = accessor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(T left, T right)
    {
      return comparator.compare(accessor.get(left), accessor.get(right));
    }
  }
}

