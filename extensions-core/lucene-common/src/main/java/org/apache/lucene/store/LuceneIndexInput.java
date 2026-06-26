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

package org.apache.lucene.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;

/**
 * Wraps an in-memory {@link ByteBuffer} as a Lucene {@link IndexInput}.
 *
 * Lucene 10 removed the (internal) ByteBufferIndexInput/ByteBufferGuard this used
 * to extend; the public replacement is ByteBuffersDataInput + ByteBuffersIndexInput.
 * Buffers must be little-endian (Lucene's on-disk byte order since 9.0).
 */
public class LuceneIndexInput
{
  private LuceneIndexInput()
  {
  }

  public static IndexInput newInstance(String resourceDescription, ByteBuffer buffer, long length)
  {
    final ByteBuffer ordered = buffer.order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffersDataInput dataInput =
        new ByteBuffersDataInput(Collections.singletonList(ordered)).slice(0, length);
    return new ByteBuffersIndexInput(dataInput, resourceDescription);
  }
}
