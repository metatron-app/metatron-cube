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

package io.druid.query.aggregation.histogram;

import com.google.common.base.Throwables;
import io.druid.common.guava.Comparators;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.java.util.common.UOE;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.MetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ApproximateHistogramFoldingSerde implements ComplexMetricSerde
{
  static final Comparator<ApproximateHistogramHolder> comparator = Comparators.NULL_FIRST(
      ApproximateHistogramAggregator.COMPARATOR);

  @Override
  public ValueDesc getType()
  {
    return ApproximateHistogramAggregatorFactory.TYPE;
  }

  public Class<? extends ApproximateHistogramHolder> getClazz()
  {
    return ApproximateHistogram.class;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      public ApproximateHistogram extract(Object rawValue)
      {
        if (rawValue == null || rawValue instanceof ApproximateHistogram) {
          return (ApproximateHistogram) rawValue;
        }
        if (rawValue instanceof String) {
          rawValue = Arrays.asList(rawValue);
        }
        if (rawValue instanceof List) {
          List dimValues = (List) rawValue;
          ApproximateHistogram h = new ApproximateHistogram();
          for (Object dimValue : dimValues) {
            if (dimValue != null) {
              h.offer((Float) ValueType.FLOAT.cast(dimValue));
            }
          }
          return h;
        }
        throw new UOE("cannot extract from [%s]", rawValue.getClass().getSimpleName());
      }
    };
  }

  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy.CompareSupport<ApproximateHistogramHolder>()
    {
      @Override
      public Class<? extends ApproximateHistogramHolder> getClazz()
      {
        return ApproximateHistogramFoldingSerde.this.getClazz();
      }

      @Override
      public ApproximateHistogramHolder fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
        readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
        try {
          return getClazz().newInstance().fromBytes(readOnlyBuffer);
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
        return h.toBytes();
      }

      @Override
      public int compare(ApproximateHistogramHolder o1, ApproximateHistogramHolder o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }
}
