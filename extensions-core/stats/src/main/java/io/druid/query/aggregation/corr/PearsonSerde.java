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

package io.druid.query.aggregation.corr;

import io.druid.common.guava.Comparators;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 */
public class PearsonSerde extends ComplexMetricSerde
{
  private static final Comparator<PearsonAggregatorCollector> comparator =
      Comparators.NULL_FIRST(PearsonAggregatorCollector.COMPARATOR);

  @Override
  public String getTypeName()
  {
    return "pearson";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<PearsonAggregatorCollector> extractedClass()
      {
        return PearsonAggregatorCollector.class;
      }

      @Override
      public PearsonAggregatorCollector extractValue(Row inputRow, String metricName)
      {
        return (PearsonAggregatorCollector) inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public ObjectStrategy<PearsonAggregatorCollector> getObjectStrategy()
  {
    return new ObjectStrategy.CompareSupport<PearsonAggregatorCollector>()
    {
      @Override
      public Class<? extends PearsonAggregatorCollector> getClazz()
      {
        return PearsonAggregatorCollector.class;
      }

      @Override
      public PearsonAggregatorCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return PearsonAggregatorCollector.from(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(PearsonAggregatorCollector collector)
      {
        return collector == null ? new byte[]{} : collector.toByteArray();
      }

      @Override
      public int compare(PearsonAggregatorCollector o1, PearsonAggregatorCollector o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
