/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram;

import com.google.common.base.Throwables;
import io.druid.segment.data.ObjectStrategy;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;

public class ApproximateHistogramBase64FoldingSerde extends ApproximateHistogramFoldingSerde
{
  @Override
  public String getTypeName()
  {
    return "approximateBase64Histogram";
  }

  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<ApproximateHistogramHolder>()
    {
      @Override
      public Class<? extends ApproximateHistogramHolder> getClazz()
      {
        return ApproximateHistogramBase64FoldingSerde.this.getClazz();
      }

      @Override
      public ApproximateHistogramHolder fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        int readSize = Math.min(buffer.remaining(), numBytes);
        final byte[] bufferForRead = new byte[readSize];
        buffer.get(bufferForRead, 0, readSize);
        try {
          return getClazz().newInstance().fromBytes(Base64.decodeBase64(bufferForRead));
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public byte[] toBytes(ApproximateHistogramHolder h)
      {
        if (h == null) {
          return new byte[]{};
        }
        return Base64.encodeBase64(h.toBytes());
      }

      @Override
      public int compare(ApproximateHistogramHolder o1, ApproximateHistogramHolder o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
