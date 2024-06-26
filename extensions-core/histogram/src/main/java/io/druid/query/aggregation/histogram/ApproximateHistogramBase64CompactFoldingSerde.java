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

import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.segment.serde.MetricExtractor;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ApproximateHistogramBase64CompactFoldingSerde extends ApproximateHistogramCompactFoldingSerde
{
  @Override
  public ValueDesc getType()
  {
    return ApproximateHistogramAggregatorFactory.COMPACT_BASE64;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      public ApproximateCompactHistogram extract(Object rawValue)
      {
        if (rawValue == null || rawValue instanceof ApproximateCompactHistogram) {
          return (ApproximateCompactHistogram) rawValue;
        }
        ApproximateCompactHistogram histogram = new ApproximateCompactHistogram();
        if (rawValue instanceof String) {
          histogram.fromBytes(StringUtils.decodeBase64((String) rawValue));
        } else if (rawValue instanceof byte[]) {
          histogram.fromBytes(StringUtils.decodeBase64((byte[]) rawValue));
        } else if (rawValue instanceof ByteBuffer) {
          ByteBuffer buffer = (ByteBuffer) rawValue;
          byte[] array;
          if (buffer.hasArray()) {
            array = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
          } else {
            array = new byte[buffer.remaining()];
            buffer.get(array);
          }
          histogram.fromBytes(StringUtils.decodeBase64(array));
        } else {
          throw new IllegalArgumentException("Not supported type " + rawValue.getClass());
        }
        return histogram;
      }
    };
  }
}
