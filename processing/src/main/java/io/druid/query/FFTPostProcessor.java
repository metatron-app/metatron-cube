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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.util.List;
import java.util.Map;

public class FFTPostProcessor extends PostProcessingOperator.Abstract
{
  private final String normalization;
  private final String transform;

  @JsonCreator
  public FFTPostProcessor(
      @JsonProperty("normalization") String normalization,
      @JsonProperty("transform") String transform
  )
  {
    this.normalization = normalization == null ? null : normalization.toUpperCase();
    this.transform = transform == null ? null : transform.toUpperCase();
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getNormalization()
  {
    return normalization;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTransform()
  {
    return transform;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      final DftNormalization n = normalization == null
                                 ? DftNormalization.STANDARD
                                 : DftNormalization.valueOf(normalization);
      final TransformType t = transform == null ? TransformType.FORWARD : TransformType.valueOf(normalization);

      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final List<Object[]> rows = Sequences.toList(baseRunner.run(query, responseContext));
        final double[] f = new double[rows.size()];
        for (int i = 0; i < f.length; i++) {
          f[i] = ((Number) rows.get(i)[0]).doubleValue();
        }
        final double[][] dataRI = new double[][]{f, new double[f.length]};
        FastFourierTransformer.transformInPlace(dataRI, n, t);
        for (int i = 0; i < f.length; i++) {
          rows.get(i)[0] = f[i];
        }
        return Sequences.simple(rows);
      }
    };
  }
}
