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

import com.google.common.base.Preconditions;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

/**
 */
public class IncrementalIndexSegment extends AbstractSegment
{
  private final IncrementalIndex index;

  public IncrementalIndexSegment(IncrementalIndex index, DataSegment descriptor)
  {
    this(index, descriptor, -1);
  }

  public IncrementalIndexSegment(IncrementalIndex index, DataSegment descriptor, int sequence)
  {
    super(descriptor, sequence);
    this.index = Preconditions.checkNotNull(index);
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  @Override
  public Interval getInterval()
  {
    return index.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex(boolean forQuery)
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter(boolean forQuery)
  {
    accessed(forQuery);
    return new IncrementalIndexStorageAdapter(index, descriptor);
  }

  @Override
  public int getNumRows()
  {
    return index.size();
  }

  @Override
  public Schema asSchema(boolean prependTime)
  {
    return index.asSchema(prependTime);
  }

  @Override
  public RowSignature asSignature(boolean prependTime)
  {
    return index.asSignature(prependTime);
  }

  @Override
  public void close()
  {
    index.close();
  }
}
