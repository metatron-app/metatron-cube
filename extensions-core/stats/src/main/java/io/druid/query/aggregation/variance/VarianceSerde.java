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

package io.druid.query.aggregation.variance;

import io.druid.common.guava.Comparators;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.UOE;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.MetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 */
public class VarianceSerde implements ComplexMetricSerde
{
  private static final Comparator<VarianceAggregatorCollector> comparator =
      Comparators.NULL_FIRST(VarianceAggregatorCollector.COMPARATOR);

  @Override
  public ValueDesc getType()
  {
    return VarianceAggregatorFactory.TYPE;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      @SuppressWarnings("unchecked")
      public VarianceAggregatorCollector extract(Object rawValue)
      {
        if (rawValue == null || rawValue instanceof VarianceAggregatorCollector) {
          return (VarianceAggregatorCollector) rawValue;
        }
        if (rawValue instanceof String) {
          rawValue = Arrays.asList(rawValue);
        }
        if (rawValue instanceof List) {
          VarianceAggregatorCollector collector = new VarianceAggregatorCollector();

          List<String> dimValues = (List) rawValue;
          for (String dimValue : dimValues) {
            collector.add(Double.parseDouble(dimValue));
          }
          return collector;
        }
        throw new UOE("cannot extract from [%s]", rawValue.getClass().getSimpleName());
      }
    };
  }

  @Override
  public ObjectStrategy<VarianceAggregatorCollector> getObjectStrategy()
  {
    return new ObjectStrategy.CompareSupport<VarianceAggregatorCollector>()
    {
      @Override
      public Class<? extends VarianceAggregatorCollector> getClazz()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return VarianceAggregatorCollector.from(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(VarianceAggregatorCollector collector)
      {
        return collector == null ? new byte[]{} : collector.toByteArray();
      }

      @Override
      public int compare(VarianceAggregatorCollector o1, VarianceAggregatorCollector o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
