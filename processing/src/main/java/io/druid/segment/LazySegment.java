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

import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.function.Supplier;

/**
 * A {@link Segment} that carries only its {@link DataSegment} metadata until it is actually queried. Identity,
 * interval and row count are answered from the descriptor (no I/O), so a historical can announce/register it in
 * the timeline WITHOUT downloading it. The underlying {@link QueryableIndex} is materialized lazily on the first
 * query access (asQueryableIndex/asStorageAdapter) via the supplied loader — e.g. fetch-from-deep-storage +
 * heap-load, with residency/eviction managed by a {@link HeapSegmentCache} behind the loader.
 *
 * Returned by a lazy {@link io.druid.segment.loading.SegmentLoader#getSegment}, so ServerManager's timeline,
 * announcement and query paths need no change.
 */
public class LazySegment extends AbstractSegment
{
  private final Supplier<QueryableIndex> loader;

  public LazySegment(DataSegment descriptor, Supplier<QueryableIndex> loader)
  {
    super(descriptor);
    this.loader = loader;
  }

  @Override
  public Interval getInterval()
  {
    return descriptor.getInterval();   // from metadata — no load
  }

  @Override
  public int getNumRows()
  {
    return descriptor.getNumRows();    // from metadata — no load
  }

  @Override
  public boolean isIndexed()
  {
    return true;
  }

  @Override
  public QueryableIndex asQueryableIndex(boolean forQuery)
  {
    accessed(forQuery);
    return loader.get();               // materialize on query access; loader/cache owns residency
  }

  @Override
  public StorageAdapter asStorageAdapter(boolean forQuery)
  {
    accessed(forQuery);
    return new QueryableIndexStorageAdapter(loader.get(), descriptor, namespace());
  }

  // schema currently needs the index; a later refinement can serve it from the container's front index
  @Override
  public Schema asSchema(boolean prependTime)
  {
    return loader.get().asSchema(prependTime);
  }

  @Override
  public RowSignature asSignature(boolean prependTime)
  {
    return loader.get().asSignature(prependTime);
  }

  @Override
  public void close()
  {
    // heap residency is owned by the cache behind the loader, not by this handle
  }
}
