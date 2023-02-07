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

import io.druid.query.Query;
import io.druid.query.RowSignature;
import io.druid.query.Schema;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public interface Segment extends SchemaProvider, Closeable
{
  String namespace();
  DataSegment getDescriptor();
  Interval getInterval();
  long getLastAccessTime();
  boolean isIndexed();
  int getNumRows();

  default String getIdentifier()
  {
    return getDescriptor().getIdentifier();
  }

  default SpecificSegmentSpec asSpec()
  {
    return new SpecificSegmentSpec(getDescriptor().toDescriptor());
  }

  default Segment cuboidFor(Query<?> query)
  {
    return null;
  }

  QueryableIndex asQueryableIndex(boolean forQuery);
  StorageAdapter asStorageAdapter(boolean forQuery);

  class Delegated implements Segment
  {
    final Segment segment;

    public Delegated(Segment segment)
    {
      this.segment = segment;
    }

    public Segment getDelegated()
    {
      return segment;
    }

    @Override
    public String namespace()
    {
      return segment.namespace();
    }

    @Override
    public DataSegment getDescriptor()
    {
      return segment.getDescriptor();
    }

    @Override
    public String getIdentifier()
    {
      return segment.getIdentifier();
    }

    @Override
    public Interval getInterval()
    {
      return segment.getInterval();
    }

    @Override
    public QueryableIndex asQueryableIndex(boolean forQuery)
    {
      return segment.asQueryableIndex(forQuery);
    }

    @Override
    public StorageAdapter asStorageAdapter(boolean forQuery)
    {
      return segment.asStorageAdapter(forQuery);
    }

    @Override
    public long getLastAccessTime()
    {
      return segment.getLastAccessTime();
    }

    @Override
    public boolean isIndexed()
    {
      return segment.isIndexed();
    }

    @Override
    public int getNumRows()
    {
      return segment.getNumRows();
    }

    @Override
    public Segment cuboidFor(Query<?> query)
    {
      return segment.cuboidFor(query);
    }

    @Override
    public Schema asSchema(boolean prependTime)
    {
      return segment.asSchema(prependTime);
    }

    @Override
    public RowSignature asSignature(boolean prependTime)
    {
      return segment.asSignature(prependTime);
    }

    @Override
    public void close() throws IOException
    {
      segment.close();
    }
  }
}
