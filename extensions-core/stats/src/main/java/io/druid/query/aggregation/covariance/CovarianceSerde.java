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

package io.druid.query.aggregation.covariance;

import com.google.common.collect.Ordering;
import io.druid.data.input.Row;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

/**
 */
public class CovarianceSerde extends ComplexMetricSerde
{
  private static final Ordering<CovarianceAggregatorCollector> comparator =
      Ordering.from(CovarianceAggregatorCollector.COMPARATOR).nullsFirst();

  @Override
  public String getTypeName()
  {
    return "covariance";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<CovarianceAggregatorCollector> extractedClass()
      {
        return CovarianceAggregatorCollector.class;
      }

      @Override
      public CovarianceAggregatorCollector extractValue(Row inputRow, String metricName)
      {
        return (CovarianceAggregatorCollector) inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public void deserializeColumn(
      ByteBuffer byteBuffer, ColumnBuilder columnBuilder
  )
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy());
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<CovarianceAggregatorCollector>()
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
