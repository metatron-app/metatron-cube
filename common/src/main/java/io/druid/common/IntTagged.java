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

package io.druid.common;

public class IntTagged<T>
{
  public static <T> IntTagged<T> of(int tag, T value)
  {
    return new IntTagged<>(tag, value);
  }

  public final int tag;
  public final T value;

  private IntTagged(int tag, T value)
  {
    this.tag = tag;
    this.value = value;
  }

  public int tag()
  {
    return tag;
  }

  public T value()
  {
    return value;
  }

  public static class Sortable<T extends Comparable> extends IntTagged<T> implements Comparable<Sortable<T>>
  {
    public Sortable(int tag, T value)
    {
      super(tag, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Sortable<T> o)
    {
      return value.compareTo(o.value);
    }
  }
}
