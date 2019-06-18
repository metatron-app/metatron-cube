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

import io.druid.data.input.Row;
import io.druid.segment.serde.ComplexMetricExtractor;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ApproximateHistogramBase64FoldingSerde extends ApproximateHistogramFoldingSerde
{
  @Override
  public String getTypeName()
  {
    return "approximateBase64Histogram";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      private final Base64 base64 = new Base64();

      @Override
      public Class<ApproximateHistogram> extractedClass()
      {
        return ApproximateHistogram.class;
      }

      @Override
      public ApproximateHistogram extractValue(Row inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null || rawValue instanceof ApproximateHistogram) {
          return (ApproximateHistogram) rawValue;
        }
        ApproximateHistogram histogram = new ApproximateHistogram();
        if (rawValue instanceof String) {
          histogram.fromBytes(base64.decode((String) rawValue));
        } else if (rawValue instanceof byte[]) {
          histogram.fromBytes(base64.decode((byte[]) rawValue));
        } else if (rawValue instanceof ByteBuffer) {
          ByteBuffer buffer = (ByteBuffer)rawValue;
          byte[] array;
          if (buffer.hasArray()) {
            array = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
          } else {
            array = new byte[buffer.remaining()];
            buffer.get(array);
          }
          histogram.fromBytes(base64.decode(array));
        } else {
          throw new IllegalArgumentException("Not supported type " + rawValue.getClass());
        }
        return histogram;
      }
    };
  }
}
