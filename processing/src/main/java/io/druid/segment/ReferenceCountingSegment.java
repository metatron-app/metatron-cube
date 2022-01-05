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
import com.google.common.collect.Maps;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.Query;
import io.druid.query.Schema;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReferenceCountingSegment extends Segment.Delegated
{
  private static final EmittingLogger log = new EmittingLogger(ReferenceCountingSegment.class);

  private int numReferences;

  public ReferenceCountingSegment(Segment segment)
  {
    super(Preconditions.checkNotNull(segment));
  }

  @Override
  public Segment cuboidFor(Query<?> query)
  {
    return numReferences < 0 ? null : segment.cuboidFor(query);
  }

  public synchronized Segment getBaseSegment()
  {
    return numReferences < 0 ? null : segment;
  }

  public synchronized int getNumReferences()
  {
    return numReferences;
  }

  public synchronized boolean isClosed()
  {
    return numReferences < 0;
  }

  @Override
  public synchronized Schema asSchema(boolean prependTime)
  {
    return numReferences < 0 ? null : super.asSchema(prependTime);
  }

  @Override
  public synchronized String getIdentifier()
  {
    return numReferences < 0 ? null : segment.getIdentifier();
  }

  @Override
  public synchronized Interval getInterval()
  {
    return numReferences < 0 ? null : segment.getInterval();
  }

  @Override
  public synchronized QueryableIndex asQueryableIndex(boolean forQuery)
  {
    return numReferences < 0 ? null : segment.asQueryableIndex(forQuery);
  }

  @Override
  public synchronized StorageAdapter asStorageAdapter(boolean forQuery)
  {
    return numReferences < 0 ? null : segment.asStorageAdapter(forQuery);
  }

  @Override
  public synchronized void close() throws IOException
  {
    if (numReferences < 0) {
      log.info("Failed to close, %s is closed already", segment);
      return;
    }

    if (numReferences > 0) {
      log.debug("%d references to %s still exist. Decrementing.", numReferences, segment);

      decrement();
    } else {
      innerClose();
    }
  }

  public synchronized Closeable increment()
  {
    if (numReferences < 0) {
      return null;
    }

    numReferences++;
    final AtomicBoolean decrementOnce = new AtomicBoolean(false);
    return new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        if (decrementOnce.compareAndSet(false, true)) {
          decrement();
        }
      }
    };
  }

  private synchronized void decrement()
  {
    if (numReferences < 0) {
      return;
    }
    if (--numReferences < 0) {
      try {
        innerClose();
      }
      catch (Exception e) {
        log.error("Unable to close queryable index %s", getIdentifier());
      }
    }
  }

  private synchronized void innerClose() throws IOException
  {
    log.debug("Closing %s, numReferences: %d", segment, numReferences);
    numReferences = -1;
    segment.close();
  }

  @Override
  public String toString()
  {
    return "ReferenceCountingSegment{" +
           "baseSegment=" + segment +
           ", numReferences=" + numReferences +
           '}';
  }

  public static class LocalSegment extends ReferenceCountingSegment
  {
    private final Map<String, Object> metaData;

    public LocalSegment(Segment baseSegment, Map<String, Object> metaData)
    {
      super(baseSegment);
      this.metaData = Maps.newLinkedHashMap(metaData);
      this.metaData.put("registered", new DateTime().toString());
    }

    public Map<String, Object> metaData()
    {
      return metaData;
    }
  }
}
