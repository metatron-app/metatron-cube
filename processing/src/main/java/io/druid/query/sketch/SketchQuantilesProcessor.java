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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.yahoo.sketches.quantiles.ItemsSketch;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("sketch.quantiles")
public class SketchQuantilesProcessor extends PostProcessingOperator.ReturnsArray
{
  private static final Logger LOG = new Logger(SketchQuantilesProcessor.class);

  private final QuantileOperation op;
  private final Map<String, double[]> fractions;
  private final Integer evenSpaced;
  private final Integer evenCounted;
  private final Integer slopedSpaced;
  private final Map<String, String[]> splitPoints;
  private final boolean dedup;

  private final Object parameter;

  @JsonCreator
  public SketchQuantilesProcessor(
      @JsonProperty("op") QuantileOperation op,
      @JsonProperty("fractions") Map<String, double[]> fractions,
      @JsonProperty("evenSpaced") Integer evenSpaced,
      @JsonProperty("evenCounted") Integer evenCounted,
      @JsonProperty("slopedSpaced") Integer slopedSpaced,
      @JsonProperty("splitPoints") Map<String, String[]> splitPoints,
      @JsonProperty("maxThreshold") int maxThreshold,
      @JsonProperty("dedup") boolean dedup
  )
  {
    this.op = op == null ? QuantileOperation.QUANTILES : op;
    this.fractions = fractions;
    this.evenSpaced = evenSpaced;
    this.evenCounted = evenCounted;
    this.slopedSpaced = slopedSpaced;
    this.splitPoints = splitPoints;
    this.dedup = dedup;

    if (op == null || op == QuantileOperation.QUANTILES) {
      parameter = fractions != null ? fractions :
                  evenSpaced != null && evenSpaced > 0 ? QuantileOperation.evenSpaced(evenSpaced, dedup) :
                  evenCounted != null && evenCounted > 0 ? QuantileOperation.evenCounted(evenCounted, dedup) :
                  slopedSpaced != null && slopedSpaced > 0 ? QuantileOperation.slopedSpaced(slopedSpaced, maxThreshold, dedup) :
                  QuantileOperation.DEFAULT_QUANTILE_PARAM;
    } else {
      parameter = splitPoints;
    }
    Preconditions.checkArgument(op == QuantileOperation.IQR || parameter != null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Object[]> postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query query, final Map responseContext)
      {
        if (!(query instanceof SketchQuery) || ((SketchQuery) query).getSketchOp() != SketchOp.QUANTILE) {
          LOG.info("query should be 'sketch' type with 'quantile' operation");
          return baseRunner.run(query, responseContext);
        }
        final SketchQuery sketchQuery = (SketchQuery) query;
        final List<String> columns = sketchQuery.estimatedOutputColumns();
        return Sequences.map(
            baseRunner.run(query, responseContext), new Function()
            {
              @Override
              public Object apply(Object input)
              {
                final Object[] sketches = (Object[]) input;
                for (int i = 1; i < sketches.length; i++) {
                  Object param = parameter instanceof Map ? ((Map)parameter).get(columns.get(i)) : parameter;
                  sketches[i] = op.calculate(((TypedSketch<ItemsSketch>) sketches[i]).value(), param);
                }
                return input;
              }
            }
        );
      }
    };
  }

  @JsonProperty
  public QuantileOperation getOp()
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

  @JsonProperty
  public boolean isDedup()
  {
    return dedup;
  }
}
