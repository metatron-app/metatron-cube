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

package io.druid.query.aggregation.hyperloglog;

import io.druid.common.utils.Murmur3;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.UOE;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.MetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 */
public class HyperUniquesSerde extends ComplexMetricSerde.CompressionSupport
{
  @Override
  public ValueDesc getType()
  {
    return HyperLogLogCollector.HLL_TYPE;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      @SuppressWarnings("unchecked")
      public HyperLogLogCollector extract(Object rawValue)
      {
        if (rawValue == null || rawValue instanceof HyperLogLogCollector) {
          return (HyperLogLogCollector) rawValue;
        }
        if (rawValue instanceof String) {
          rawValue = Arrays.asList(rawValue);
        }
        if (rawValue instanceof List) {
          HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
          List<String> dimValues = (List) rawValue;
          for (String dimensionValue : dimValues) {
            collector.add(Murmur3.hash128(StringUtils.toUtf8(dimensionValue)));
          }
          return collector;
        }
        throw new UOE("cannot extract from [%s]", rawValue.getClass().getSimpleName());
      }
    };
  }

  @Override
  public ObjectStrategy<HyperLogLogCollector> getObjectStrategy()
  {
    return new ObjectStrategy<HyperLogLogCollector>()
    {
      @Override
      public Class<? extends HyperLogLogCollector> getClazz()
      {
        return HyperLogLogCollector.class;
      }

      @Override
      public HyperLogLogCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return HyperLogLogCollector.from(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(HyperLogLogCollector collector)
      {
        return collector == null ? StringUtils.EMPTY_BYTES : collector.toByteArray();
      }
    };
  }
}
