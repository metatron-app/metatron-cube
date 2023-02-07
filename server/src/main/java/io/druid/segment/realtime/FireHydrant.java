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

package io.druid.segment.realtime;

import com.google.common.base.Throwables;
import io.druid.java.util.common.Pair;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.Closeable;
import java.io.File;

/**
 */
public class FireHydrant
{
  private final int count;
  private IncrementalIndex index;
  private ReferenceCountingSegment adapter;
  private File persistedPath;
  private long persistingTime;
  private final Object swapLock = new Object();

  public FireHydrant(IncrementalIndexSegment index, int count)
  {
    this.index = index.getIndex();
    this.adapter = new ReferenceCountingSegment(index);
    this.count = count;
  }

  public FireHydrant(QueryableIndexSegment adapter, File persistedPath, int count)
  {
    this.index = null;
    this.adapter = new ReferenceCountingSegment(adapter);
    this.persistedPath = persistedPath;
    this.persistingTime = -1; // unknown
    this.count = count;
  }

  public int rowCountInMemory()
  {
    synchronized (swapLock) {
      return index == null ? 0 : index.size();
    }
  }

  public long estimatedOccupation()
  {
    synchronized (swapLock) {
      return index == null ? 0 : index.estimatedOccupation();
    }
  }

  public boolean canAppendRow()
  {
    synchronized (swapLock) {
      return index != null && index.canAppendRow();
    }
  }

  public IncrementalIndex getIndex()
  {
    synchronized (swapLock) {
      return index;
    }
  }

  public Segment getSegment()
  {
    synchronized (swapLock) {
      return adapter;
    }
  }

  public int getCount()
  {
    return count;
  }

  public long getPersistingTime()
  {
    synchronized (swapLock) {
      return persistingTime;
    }
  }

  public File getPersistedPath()
  {
    synchronized (swapLock) {
      return persistedPath;
    }
  }

  public boolean hasSwapped()
  {
    synchronized (swapLock) {
      return index == null;
    }
  }

  public void persisted(QueryableIndex index, File persistedPath, long persistingTime)
  {
    final QueryableIndexSegment swapping = new QueryableIndexSegment(index, adapter.getDescriptor(), count);
    synchronized (swapLock) {
      try {
        adapter.close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      this.adapter = new ReferenceCountingSegment(swapping);
      this.persistingTime = persistingTime;
      this.persistedPath = persistedPath;
      this.index = null;
    }
  }

  public Pair<Segment, Closeable> getAndIncrementSegment()
  {
    // Prevent swapping of index before increment is called
    synchronized (swapLock) {
      return new Pair<Segment, Closeable>(adapter, adapter.increment());
    }
  }

  @Override
  public String toString()
  {
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter +
           ", count=" + count +
           '}';
  }
}
