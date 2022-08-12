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

package io.druid.segment.data;

import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.BytesOutputStream;
import io.druid.segment.data.ObjectStrategy.RawComparable;
import io.druid.segment.data.ObjectStrategy.SingleThreadSupport;

import java.nio.ByteBuffer;

public class ObjectStrategies
{
  public static <T> ObjectStrategy<T> singleThreaded(ObjectStrategy<T> strategy)
  {
    if (strategy instanceof SingleThreadSupport) {
      return ((SingleThreadSupport<T>) strategy).singleThreaded();
    }
    return strategy;
  }

  static class StringObjectStrategy implements RawComparable<String>, SingleThreadSupport<String>
  {
    private static final int SCRATCH_LIMIT = 1024;

    @Override
    public Class<? extends String> getClazz()
    {
      return String.class;
    }

    @Override
    public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
    {
      return StringUtils.fromUtf8(buffer, numBytes);
    }

    @Override
    public byte[] toBytes(String val)
    {
      return StringUtils.toUtf8WithNullToEmpty(val);
    }

    @Override
    public int compare(String o1, String o2)
    {
      return GuavaUtils.nullFirstNatural().compare(o1, o2);
    }

    @Override
    public ObjectStrategy<String> singleThreaded()
    {
      return new StringObjectStrategy()
      {
        private final BytesOutputStream scratch = new BytesOutputStream();

        @Override
        public String fromByteBuffer(final ByteBuffer buffer, final int numBytes)
        {
          return numBytes > SCRATCH_LIMIT
                 ? StringUtils.fromUtf8(buffer, numBytes)
                 : StringUtils.fromUtf8(buffer, numBytes, scratch);
        }
      };
    }
  }
}
