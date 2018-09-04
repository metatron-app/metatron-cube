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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.frequencies.ItemsSketch;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;

import java.util.Map;

/**
 */
@JsonTypeName("sketch.frequency")
public class SketchFrequencyProcessor extends PostProcessingOperator.Abstract
{
  private static final Logger LOG = new Logger(SketchFrequencyProcessor.class);

  @JsonCreator
  public SketchFrequencyProcessor() {}

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        if (!(query instanceof SketchQuery) || ((SketchQuery) query).getSketchOp() != SketchOp.FREQUENCY) {
          LOG.info("query should be 'frequency' type with 'theta' operation");
          return baseRunner.run(query, responseContext);
        }
        return Sequences.map(
            baseRunner.run(query, responseContext), new Function()
            {
              @Override
              public Object apply(Object input)
              {
                Result<Map<String, Object>> element = (Result<Map<String, Object>>) input;
                Map<String, Object> result = element.getValue();
                for (Map.Entry<String, Object> entry : result.entrySet()) {
                  Map<String, Object> columns = Maps.newLinkedHashMap();
                  TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) entry.getValue();
                  for (ItemsSketch.Row row : sketch.value().getFrequentItems(ErrorType.NO_FALSE_NEGATIVES)) {
                    columns.put((String) row.getItem(), row.getEstimate());
                  }
                  entry.setValue(columns);
                }
                return input;
              }
            }
        );
      }
    };
  }
}
