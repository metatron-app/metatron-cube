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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;

import java.util.Map;

/**
 */
@JsonTypeName("sketch.theta")
public class SketchThetaProcessor extends PostProcessingOperator.ReturnsArray
{
  private static final Logger LOG = new Logger(SketchThetaProcessor.class);

  @JsonCreator
  public SketchThetaProcessor() {}

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Object[]> postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner<Object[]>()
    {
      @Override
      public Sequence<Object[]> run(Query query, Map responseContext)
      {
        if (!(query instanceof SketchQuery) || ((SketchQuery) query).getSketchOp() != SketchOp.THETA) {
          LOG.info("query should be 'sketch' type with 'theta' operation");
          return baseRunner.run(query, responseContext);
        }
        return Sequences.map(
            baseRunner.run(query, responseContext), new Function()
            {
              @Override
              public Object apply(Object input)
              {
                Object[] result = (Object[]) input;
                for (int i = 1; i < result.length; i++) {
                  if (result[i] instanceof TypedSketch) {
                    TypedSketch<Sketch> sketch = (TypedSketch<Sketch>) result[i];
                    result[i] = SketchThetaPostAggregator.toMap(sketch.value(), 2, false);
                  }
                }
                return input;
              }
            }
        );
      }
    };
  }
}
