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

package io.druid.query.aggregation.area;

import io.druid.common.guava.Comparators;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class MetricAreaSerde implements ComplexMetricSerde
{
  private static final Comparator<MetricArea> comparator = Comparators.NULL_FIRST(MetricAreaAggregator.COMPARATOR);

  private final ObjectStrategy<MetricArea> strategy = new ObjectStrategy.CompareSupport<MetricArea>()
  {
    @Override
    public Class<MetricArea> getClazz()
    {
      return MetricArea.class;
    }

    @Override
    public MetricArea fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
      readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
      return MetricArea.fromBytes(readOnlyBuffer);
    }

    @Override
    public byte[] toBytes(MetricArea val)
    {
      if (val == null) {
        return new byte[]{};
      }
      return val.toBytes();
    }

    @Override
    public int compare(MetricArea o1, MetricArea o2)
    {
      return comparator.compare(o1, o2);
    }
  };

  @Override
  public ValueDesc getType()
  {
    return MetricAreaAggregatorFactory.TYPE;
  }

  @Override
  public ComplexMetricExtractor getExtractor(List<String> typeHint)
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public MetricArea extractValue(Row inputRow, String metricName)
      {
        final Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null || rawValue instanceof MetricArea) {
          return (MetricArea) rawValue;
        } else {
          return new MetricArea().add(Rows.parseDouble(rawValue));
        }
      }
    };
  }

  @Override
  public ObjectStrategy<MetricArea> getObjectStrategy()
  {
    return strategy;
  }
}
