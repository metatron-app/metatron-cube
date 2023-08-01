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
import io.druid.data.ValueType;
import io.druid.java.util.common.UOE;
import io.druid.segment.serde.MetricExtractor;

import java.util.Arrays;
import java.util.List;

/**
 *
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
        if (rawValue instanceof String) {
          rawValue = Arrays.asList(rawValue);
        }
        if (rawValue instanceof List) {
          List dimValues = (List) rawValue;
          ApproximateCompactHistogram h = new ApproximateCompactHistogram();
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
}
