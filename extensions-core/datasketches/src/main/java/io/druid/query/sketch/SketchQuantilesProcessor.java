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
import com.fasterxml.jackson.annotation.JsonInclude;
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
public class SketchQuantilesProcessor extends PostProcessingOperator.Abstract
{
  private static final Logger LOG = new Logger(SketchQuantilesProcessor.class);

  private final SketchQuantilesOp op;
  private final Map<String, double[]> fractions;
  private final Integer evenSpaced;
  private final Integer evenCounted;
  private final Integer slopedSpaced;
  private final Map<String, String[]> splitPoints;

  private final Object parameter;

  @JsonCreator
  public SketchQuantilesProcessor(
      @JsonProperty("op") SketchQuantilesOp op,
      @JsonProperty("fractions") Map<String, double[]> fractions,
      @JsonProperty("evenSpaced") Integer evenSpaced,
      @JsonProperty("evenCounted") Integer evenCounted,
      @JsonProperty("slopedSpaced") Integer slopedSpaced,
      @JsonProperty("splitPoints") Map<String, String[]> splitPoints
  )
  {
    this.op = op == null ? SketchQuantilesOp.QUANTILES : op;
    this.fractions = fractions;
    this.evenSpaced = evenSpaced;
    this.evenCounted = evenCounted;
    this.slopedSpaced = slopedSpaced;
    this.splitPoints = splitPoints;

    if (op == null || op == SketchQuantilesOp.QUANTILES) {
      parameter = fractions != null ? fractions :
                  evenSpaced != null && evenSpaced > 0 ? SketchQuantilesOp.evenSpaced(evenSpaced) :
                  evenCounted != null && evenCounted > 0 ? SketchQuantilesOp.evenCounted(evenCounted) :
                  slopedSpaced != null && slopedSpaced > 0 ? SketchQuantilesOp.slopedSpaced(slopedSpaced) :
                  SketchQuantilesOp.DEFAULT_QUANTILE_PARAM;
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
                  TypedSketch<ItemsSketch> sketch = (TypedSketch<ItemsSketch>) entry.getValue();
                  entry.setValue(op.calculate(sketch.value(), param));
                }
                return input;
              }
            }
        );
      }
    };
  }

  @JsonProperty
  public SketchQuantilesOp getOp()
  {
    return op;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, double[]> getFractions()
  {
    return fractions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getEvenSpaced()
  {
    return evenSpaced;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getEvenCounted()
  {
    return evenCounted;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getSlopedSpaced()
  {
    return slopedSpaced;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, String[]> getSplitPoints()
  {
    return splitPoints;
  }
}
