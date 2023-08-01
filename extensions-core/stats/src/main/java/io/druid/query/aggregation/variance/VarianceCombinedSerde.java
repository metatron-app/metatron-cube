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

import io.druid.data.ValueType;
import io.druid.java.util.common.UOE;
import io.druid.segment.serde.MetricExtractor;

import java.util.Arrays;
import java.util.List;

public class VarianceCombinedSerde extends VarianceSerde
{
  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    return new MetricExtractor()
    {
      @Override
      public VarianceAggregatorCollector extract(Object rawValue)
      {
        if (rawValue == null || rawValue instanceof VarianceAggregatorCollector) {
          return (VarianceAggregatorCollector) rawValue;
        } else if (rawValue instanceof String) {
          VarianceAggregatorCollector variance = toVariance((String) rawValue);
          if (variance != null) {
            return variance;
          }
          rawValue = Arrays.asList(rawValue);
        }
        if (rawValue instanceof List) {
          VarianceAggregatorCollector collector = new VarianceAggregatorCollector();

          List dimValues = (List) rawValue;
          for (Object dimValue : dimValues) {
            if (dimValue != null) {
              collector.add((Double) ValueType.DOUBLE.cast(dimValue));
            }
          }
          return collector;
        }
        throw new UOE("cannot extract from [%s]", rawValue.getClass().getSimpleName());
      }
    };
  }

  // lessen cost of input.split(",")
  private VarianceAggregatorCollector toVariance(String input)
  {
    final int index1 = input.indexOf(',');
    final int index2 = input.indexOf(',', index1 + 1);
    if (index1 < 0 || index2 < 0) {
      return null;
    }
    double nvar = Double.parseDouble(input.substring(0, index1).trim());
    long count = Long.parseLong(input.substring(index1 + 1, index2).trim());
    double sum = Double.parseDouble(input.substring(index2 + 1).trim());
    return new VarianceAggregatorCollector(count, sum, nvar);
  }
}
