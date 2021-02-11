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

package io.druid.query.groupby.orderby;

import java.util.Iterator;
import java.util.Queue;

/**
 * Utility class that supports iterating a priority queue in sorted order.
 */
public class OrderedPriorityQueueItems<T> implements Iterator<T>
{
  private final Queue<T> rows;

  public OrderedPriorityQueueItems(Queue<T> rows)
  {
    this.rows = rows;
  }

  @Override
  public boolean hasNext()
  {
    return !rows.isEmpty();
  }

  @Override
  public T next()
  {
    return rows.poll();
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException("Can't remove any item from an intermediary heap for orderBy/limit");
  }
}
