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

package io.druid.query.aggregation.covariance;

import io.druid.common.guava.Comparators;
import io.druid.data.ValueDesc;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 */
public class CovarianceSerde implements ComplexMetricSerde
{
  private static final Comparator<CovarianceAggregatorCollector> comparator =
      Comparators.NULL_FIRST(CovarianceAggregatorCollector.COMPARATOR);

  @Override
  public ValueDesc getType()
  {
    return CovarianceAggregatorFactory.TYPE;
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy.CompareSupport<CovarianceAggregatorCollector>()
    {
      @Override
      public Class<? extends CovarianceAggregatorCollector> getClazz()
      {
        return CovarianceAggregatorCollector.class;
      }

      @Override
      public CovarianceAggregatorCollector fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        return CovarianceAggregatorCollector.from(readOnlyBuffer);
      }

      @Override
      public byte[] toBytes(CovarianceAggregatorCollector collector)
      {
        return collector == null ? new byte[]{} : collector.toByteArray();
      }

      @Override
      public int compare(CovarianceAggregatorCollector o1, CovarianceAggregatorCollector o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
