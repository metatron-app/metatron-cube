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

package io.druid.common.guava;

import com.google.common.collect.Iterators;
import io.druid.java.util.common.UOE;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

public class ListWrap<E> implements List<E>
{
  private final E v;

  public ListWrap(E v) {this.v = v;}

  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public boolean isEmpty()
  {
    return false;
  }

  @Override
  public boolean contains(Object o)
  {
    return Objects.equals(v, o);
  }

  @Override
  public Iterator<E> iterator()
  {
    return Iterators.singletonIterator(v);
  }

  @Override
  public Object[] toArray()
  {
    return new Object[]{v};
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a)
  {
    final T[] r = a.length >= 1 ? a : (T[]) Array.newInstance(a.getClass().getComponentType(), 1);
    r[0] = (T) v;
    return r;
  }

  @Override
  public boolean add(E e)
  {
    throw new UOE("add");
  }

  @Override
  public boolean remove(Object o)
  {
    throw new UOE("remove");
  }

  @Override
  public boolean containsAll(Collection<?> c)
  {
    return c.isEmpty() || c.size() == 1 && Objects.equals(v, c.iterator().next());
  }

  @Override
  public boolean addAll(Collection<? extends E> c)
  {
    throw new UOE("addAll");
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c)
  {
    throw new UOE("addAll");
  }

  @Override
  public boolean removeAll(Collection<?> c)
  {
    throw new UOE("removeAll");
  }

  @Override
  public boolean retainAll(Collection<?> c)
  {
    throw new UOE("retainAll");
  }

  @Override
  public void clear()
  {
    throw new UOE("clear");
  }

  @Override
  public E get(int index)
  {
    if (index > 0) {
      throw new IndexOutOfBoundsException();
    }
    return v;
  }

  @Override
  public E set(int index, E element)
  {
    throw new UOE("set");
  }

  @Override
  public void add(int index, E element)
  {
    throw new UOE("add");
  }

  @Override
  public E remove(int index)
  {
    throw new UOE("remove");
  }

  @Override
  public int indexOf(Object o)
  {
    return v.equals(o) ? 0 : -1;
  }

  @Override
  public int lastIndexOf(Object o)
  {
    return v.equals(o) ? 0 : -1;
  }

  @Override
  public ListIterator<E> listIterator()
  {
    throw new UOE("listIterator");
  }

  @Override
  public ListIterator<E> listIterator(int index)
  {
    throw new UOE("listIterator");
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex)
  {
    throw new UOE("subList");
  }
}
