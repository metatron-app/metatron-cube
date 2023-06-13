/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.collections;

import io.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public final class BufferPool extends StupidPool<ByteBuffer>
{
  private static final Logger log = new Logger(BufferPool.class);

  public static BufferPool heap(final int bufferSize)
  {
    return heap(bufferSize, Integer.MAX_VALUE);
  }

  public static BufferPool heap(final int bufferSize, int numObjects)
  {
    return new BufferPool(bufferSize, false, numObjects);
  }

  public static BufferPool direct(final int bufferSize)
  {
    return direct(bufferSize, Integer.MAX_VALUE);
  }

  public static BufferPool direct(final int bufferSize, int numObjects)
  {
    return new BufferPool(bufferSize, true, numObjects);
  }

  private int bufferSize;
  private final AtomicLong count = new AtomicLong(0);

  private BufferPool(int bufferSize, boolean direct, int numObjects)
  {
    super(direct ? () -> ByteBuffer.allocateDirect(bufferSize) : () -> ByteBuffer.allocate(bufferSize));
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  @Override
  protected ByteBuffer generate(String source)
  {
    log.debug("Allocating new buffer[%,d] of size[%,d] for [%s]", count.getAndIncrement(), bufferSize, source);
    return super.generate(source);
  }

  @Override
  protected ByteBuffer prepare(ByteBuffer obj, String source)
  {
    return (ByteBuffer) obj.clear();
  }
}
