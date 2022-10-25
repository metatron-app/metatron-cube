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

import io.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CachingIterator<T> implements CloseableIterator<T>
{
  private final Iterator<T> iterator;

  private final Object[] cached;
  private int limit;
  private int offset;

  public CachingIterator(Iterator<T> iterator, int cache)
  {
    this.iterator = iterator;
    this.cached = new Object[cache];
  }

  @Override
  public boolean hasNext()
  {
    return offset < limit || iterator.hasNext();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T next()
  {
    if (offset == limit) {
      int i = 0;
      for (; iterator.hasNext() && i < cached.length; i++) {
        cached[i] = iterator.next();
      }
      if (i == 0) {
        throw new NoSuchElementException();
      }
      offset = 0;
      limit = i;
    }
    return (T) cached[offset++];
  }

  @Override
  public void close() throws IOException
  {
    Arrays.fill(cached, null);
    if (iterator instanceof Closeable) {
      ((Closeable) iterator).close();
    }
  }
}