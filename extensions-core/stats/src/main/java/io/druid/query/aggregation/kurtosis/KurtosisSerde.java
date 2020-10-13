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

package io.druid.query.aggregation.kurtosis;

import io.druid.common.guava.Comparators;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 */
public class KurtosisSerde extends ComplexMetricSerde
{
  private static final Comparator<KurtosisAggregatorCollector> comparator =
      Comparators.NULL_FIRST(KurtosisAggregatorCollector.COMPARATOR);

  @Override
  public String getTypeName()
  {
    return "kurtosis";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<KurtosisAggregatorCollector> extractedClass()
      {
        return KurtosisAggregatorCollector.class;
      }

      @Override
      public KurtosisAggregatorCollector extractValue(Row inputRow, String metricName)
      {
        return (KurtosisAggregatorCollector) inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public ObjectStrategy<KurtosisAggregatorCollector> getObjectStrategy()
  {
    return new ObjectStrategy.CompareSupport<KurtosisAggregatorCollector>()
    {
      @Override
      public Class<? extends KurtosisAggregatorCollector> getClazz()
      {
        return KurtosisAggregatorCollector.class;
      }

      @Override
      public KurtosisAggregatorCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return KurtosisAggregatorCollector.from(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(KurtosisAggregatorCollector collector)
      {
        return collector == null ? new byte[]{} : collector.toByteArray();
      }

      @Override
      public int compare(KurtosisAggregatorCollector o1, KurtosisAggregatorCollector o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
