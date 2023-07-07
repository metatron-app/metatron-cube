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

import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.segment.serde.ComplexMetricExtractor;

import java.util.Iterator;
import java.util.List;

/**
 */
public class ApproximateHistogramCompactFoldingSerde extends ApproximateHistogramFoldingSerde
{
  @Override
  public ValueDesc getType()
  {
    return ApproximateHistogramAggregatorFactory.COMPACT;
  }

  @Override
  public Class<? extends ApproximateHistogramHolder> getClazz()
  {
    return ApproximateCompactHistogram.class;
  }

  @Override
  public ComplexMetricExtractor getExtractor(List<String> typeHint)
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public ApproximateCompactHistogram extractValue(Row inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof ApproximateCompactHistogram) {
          return (ApproximateCompactHistogram) rawValue;
        } else {
          List<String> dimValues = inputRow.getDimension(metricName);
          if (dimValues != null && dimValues.size() > 0) {
            Iterator<String> values = dimValues.iterator();

            ApproximateCompactHistogram h = new ApproximateCompactHistogram();

            while (values.hasNext()) {
              float value = Float.parseFloat(values.next());
              h.offer(value);
            }
            return h;
          } else {
            return new ApproximateCompactHistogram(0);
          }
        }
      }
    };
  }
}
