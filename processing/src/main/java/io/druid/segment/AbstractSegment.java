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
import io.druid.timeline.DataSegment;

import java.io.IOException;

public abstract class AbstractSegment implements Segment
{
  protected final DataSegment descriptor;
  protected final int sequence;
  protected volatile long lastAccessTime;

  protected AbstractSegment(DataSegment descriptor)
  {
    this(descriptor, -1);
  }

  protected AbstractSegment(DataSegment descriptor, int sequence)
  {
    this.descriptor = Preconditions.checkNotNull(descriptor, "descriptor");
    this.sequence = sequence;
  }

  @Override
  public String namespace()
  {
    return sequence < 0 ? descriptor.getIdentifier() : descriptor.getIdentifier() + '_' + sequence;
  }

  @Override
  public DataSegment getDescriptor()
  {
    return descriptor;
  }

  protected void accessed(boolean forQuery)
  {
    if (forQuery) {
      lastAccessTime = System.currentTimeMillis();
    }
  }

  @Override
  public long getLastAccessTime()
  {
    return lastAccessTime;
  }

  public void close() throws IOException {}

  @Override
  public String toString()
  {
    return getIdentifier();
  }
}
