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

package io.druid.query.aggregation.variance;

import io.druid.data.input.InputRow;
import io.druid.segment.serde.ComplexMetricExtractor;

import java.util.List;

public class VarianceCombinedSerde extends VarianceSerde
{
  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<VarianceAggregatorCollector> extractedClass()
      {
        return VarianceAggregatorCollector.class;
      }

      @Override
      public VarianceAggregatorCollector extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue instanceof VarianceAggregatorCollector) {
          return (VarianceAggregatorCollector) rawValue;
        } else if (rawValue instanceof String) {
          String strValue = (String)rawValue;
          String[] params = strValue.split(",");
          if (params.length == 3) {
            double nvar = Double.parseDouble(params[0].trim());
            long count = Long.parseLong(params[1].trim());
            double sum = Double.parseDouble(params[2].trim());
            return new VarianceAggregatorCollector(count, sum, nvar);
          }
        }
        VarianceAggregatorCollector collector = new VarianceAggregatorCollector();

        List<String> dimValues = inputRow.getDimension(metricName);
        if (dimValues != null && dimValues.size() > 0) {
          for (String dimValue : dimValues) {
            double value = Double.parseDouble(dimValue);
            collector.add(value);
          }
        }
        return collector;
      }
    };
  }
}
