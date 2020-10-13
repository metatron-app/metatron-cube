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

package io.druid.query.aggregation.range;

import io.druid.common.guava.Comparators;
import io.druid.data.Rows;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class MetricRangeSerde extends ComplexMetricSerde
{
  private static Comparator<MetricRange> comparator = Comparators.NULL_FIRST(
      (mr1, mr2) -> MetricRangeAggregator.COMPARATOR.compare(mr1, mr2)
  );

  private ObjectStrategy<MetricRange> strategy = new ObjectStrategy.CompareSupport<MetricRange>()
  {
    @Override
    public Class<MetricRange> getClazz()
    {
      return MetricRange.class;
    }

    @Override
    public MetricRange fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return MetricRange.fromBytes(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(MetricRange val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(MetricRange o1, MetricRange o2)
    {
      return comparator.compare(o1, o2);
    }
  };

  @Override
  public String getTypeName()
  {
    return "metricRange";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return MetricRange.class;
      }

      @Override
      public MetricRange extractValue(Row inputRow, String metricName)
      {
        final Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null || rawValue instanceof MetricRange) {
          return (MetricRange) rawValue;
        } else {
          return new MetricRange().add(Rows.parseDouble(rawValue));
        }
      }
    };
  }

  @Override
  public ObjectStrategy<MetricRange> getObjectStrategy()
  {
    return strategy;
  }
}
