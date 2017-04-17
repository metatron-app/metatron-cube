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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;

import java.util.Map;

/**
 */
@JsonTypeName("sketch.quantiles")
public class SketchQuantilesProcessor implements PostProcessingOperator
{
  private static final Logger LOG = new Logger(SketchQuantilesProcessor.class);

  private final SketchQuantilesOp op;
  private final Object parameter;

  @JsonCreator
  public SketchQuantilesProcessor(
      @JsonProperty("op") SketchQuantilesOp op,
      @JsonProperty("fractions") Map<String, double[]> fractions,
      @JsonProperty("evenSpaced") Integer evenSpaced,
      @JsonProperty("evenCounted") Integer evenCounted,
      @JsonProperty("splitPoints") Map<String, String[]> splitPoints
  )
  {
    this.op = op == null ? SketchQuantilesOp.QUANTILES : op;
    if (op == null || op == SketchQuantilesOp.QUANTILES) {
      parameter = fractions != null ? fractions :
                  evenSpaced != null && evenSpaced > 0 ? evenSpaced :
                  evenCounted != null && evenCounted > 0 ? -evenCounted : 11;
    } else {
      parameter = splitPoints;
    }
    Preconditions.checkArgument(op == SketchQuantilesOp.IQR || parameter != null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        if (!(query instanceof SketchQuery) || ((SketchQuery) query).getSketchOp() != SketchOp.QUANTILE) {
          LOG.info("query should be 'sketch' type with 'quantile' operation");
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
                  Object param = parameter instanceof Map ? ((Map)parameter).get(entry.getKey()) : parameter;
                  entry.setValue(op.calculate((ItemsSketch) entry.getValue(), param));
                }
                return input;
              }
            }
        );
      }
    };
  }
}
