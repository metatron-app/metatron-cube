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

import io.druid.query.SegmentDescriptor;
import io.druid.query.select.Schema;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;

/**
 */
public interface Segment extends SchemaProvider, Closeable
{
  String getIdentifier();
  Interval getDataInterval();
  QueryableIndex asQueryableIndex(boolean forQuery);
  StorageAdapter asStorageAdapter(boolean forQuery);
  long getLastAccessTime();

  class WithDescriptor implements Segment
  {
    private final Segment segment;
    private final SegmentDescriptor descriptor;

    public WithDescriptor(Segment segment, SegmentDescriptor descriptor) {
      this.segment = segment;
      this.descriptor = descriptor;
    }

    public Segment getSegment()
    {
      return segment;
    }

    public SegmentDescriptor getDescriptor()
    {
      return descriptor;
    }

    @Override
    public String getIdentifier()
    {
      return segment.getIdentifier();
    }

    @Override
    public Interval getDataInterval()
    {
      return segment.getDataInterval();
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
    public Schema asSchema(boolean prependTime)
    {
      return segment.asSchema(prependTime);
    }

    @Override
    public void close() throws IOException
    {
      segment.close();
    }
  }
}
