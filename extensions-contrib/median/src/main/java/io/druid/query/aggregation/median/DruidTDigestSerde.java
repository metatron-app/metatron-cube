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

package io.druid.query.aggregation.median;

import io.druid.common.guava.Comparators;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class DruidTDigestSerde implements ComplexMetricSerde
{
  private static Comparator<DruidTDigest> comparator = Comparators.NULL_FIRST(DruidTDigestAggregator.COMPARATOR);

  @Override
  public String getTypeName()
  {
    return "DruidTDigest";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public DruidTDigest extractValue(Row inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof DruidTDigest) {
          return (DruidTDigest) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            DruidTDigest digest = new DruidTDigest(DruidTDigestAggregator.DEFAULT_COMPRESSION);

            while (values.hasNext()) {
              double value = Double.parseDouble(values.next());
              digest.add(value);
            }
            return digest;
          } else {
            return new DruidTDigest(DruidTDigestAggregator.DEFAULT_COMPRESSION);
          }
        }
      }
    };
  }

  @Override
  public ObjectStrategy<DruidTDigest> getObjectStrategy()
  {
    return new ObjectStrategy<DruidTDigest>()
    {
      @Override
      public Class<? extends DruidTDigest> getClazz()
      {
        return DruidTDigest.class;
      }

      @Override
      public DruidTDigest fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return DruidTDigest.fromBytes(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(DruidTDigest digest)
      {
        if (digest == null) {
          return new byte[]{};
        }

        return digest.toBytes();
      }
    };
  }
}
