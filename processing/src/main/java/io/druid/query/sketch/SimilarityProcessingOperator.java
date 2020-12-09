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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yahoo.sketches.theta.Sketch;
import io.druid.common.guava.Accumulator;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.UnionAllQueryRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.druid.query.sketch.ThetaOperations.Func.INTERSECT;
import static io.druid.query.sketch.ThetaOperations.Func.NOT;
import static io.druid.query.sketch.ThetaOperations.Func.UNION;

/**
 */
@JsonTypeName("similarity")
public class SimilarityProcessingOperator implements PostProcessingOperator.UnionSupport
{
  private static final Logger LOG = new Logger(SimilarityProcessingOperator.class);

  private final float threshold;
  private final Set<String> dataSourceSet;

  @JsonCreator
  public SimilarityProcessingOperator(
      @JsonProperty("threshold") float threshold,
      @JsonProperty("dataSources") List<String> dataSources
  )
  {
    this.threshold = threshold;
    this.dataSourceSet = dataSources == null || dataSources.isEmpty() ? null : Sets.newHashSet(dataSources);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public float getThreshold()
  {
    return threshold;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getDataSourceSet()
  {
    return Lists.newArrayList(dataSourceSet);
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        if (!(query instanceof SketchQuery) ||
            ((SketchQuery) query).getSketchOp() != SketchOp.THETA) {
          LOG.info("query should be 'sketch' type with 'theta' operation");
          return baseRunner.run(query, responseContext);
        }
        final SketchQuery sketchQuery = (SketchQuery) query;
        final List<String> columns = sketchQuery.estimatedOutputColumns();
        final int nomEntries = sketchQuery.getSketchParamWithDefault();
        final List<Similarity> similarities = Lists.newArrayList();
        Sequence<Object[]> sequences = baseRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Object[]>()
            {
              @Override
              public Object accumulate(Object accumulated, Object[] result)
              {
                final Map<String, Sketch> sketchMap = Maps.newHashMapWithExpectedSize(result.length);
                for (int i = 1; i < result.length; i++) {
                  String column = columns.get(i);
                  TypedSketch<Sketch> sketch = (TypedSketch<Sketch>) result[i];
                  for (Map.Entry<String, Sketch> sketches : sketchMap.entrySet()) {
                    Map<String, Object> relations = getSimilarity(nomEntries, sketch.value(), sketches.getValue());
                    double similarity = (double) relations.get("similarity");
                    if (similarity > threshold) {
                      similarities.add(new Similarity(column, sketches.getKey(), relations));
                    }
                  }
                  sketchMap.put(column, sketch.value());
                }
                return null;
              }
            }
        );
        return Sequences.simple(similarities);
      }
    };
  }

  // ds1.dim1 --> ds2.dim3 : 0.66f
  // ds1.dim2 --> ds3.dim4 : 0.66f
  //
  // ds1, {dim1=ds2.dim3:0.66f, dim2=ds3.dim4:0.28f}
  @Override
  public QueryRunner postProcess(final UnionAllQueryRunner baseRunner, final ExecutorService exec)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final Query representative = BaseQuery.getRepresentative(query);
        if (!(representative instanceof SketchQuery) ||
            ((SketchQuery) representative).getSketchOp() != SketchOp.THETA) {
          LOG.info("query should be 'sketch' type with 'theta' operation");
          return baseRunner.run(query, responseContext);
        }
        final int nomEntries = ((SketchQuery) representative).getSketchParamWithDefault();
        final Map<String, Map<String, Sketch>> sketches = Maps.newHashMap();
        final List<Similarity> similarities = Lists.newArrayList();
        Sequence<Pair<Query, Sequence>> sequences = baseRunner.run(query, responseContext);
        sequences.accumulate(
            null, new Accumulator<Object, Pair<Query, Sequence>>()
            {
              @Override
              public Object accumulate(
                  Object accumulated, Pair<Query, Sequence> in
              )
              {
                final String dataSource = Iterables.getOnlyElement(in.lhs.getDataSource().getNames());
                final SketchQuery sketchQuery = (SketchQuery) in.lhs;
                final Sequence sequence = in.rhs;

                final List<String> columns = sketchQuery.estimatedOutputColumns();
                sequence.accumulate(
                    null, new Accumulator<Object, Object>()
                    {
                      @Override
                      public Object accumulate(Object accumulated, Object input)
                      {
                        final Object[] result = (Object[]) input;
                        final boolean except = dataSourceSet != null && !dataSourceSet.contains(dataSource);
                        final Map<String, Sketch> sketchMap = Maps.newHashMapWithExpectedSize(result.length);
                        for (int i = 1; i < result.length; i++) {
                          if (result[i] == null) {
                            continue;   // empty or not-existing
                          }
                          TypedSketch<Sketch> sketch = (TypedSketch<Sketch>) result[i];
                          sketchMap.put(columns.get(i), sketch.value());
                        }

                        for (Map.Entry<String, Map<String, Sketch>> prev : sketches.entrySet()) {
                          String ds = prev.getKey();
                          if (except && !dataSourceSet.contains(ds)) {
                            continue;
                          }
                          for (Map.Entry<String, Sketch> entry : sketchMap.entrySet()) {
                            final String columnName = entry.getKey();
                            final Sketch sketch = entry.getValue();
                            double maxSimilarity = -1;
                            String maxSimilarDimension = null;
                            Map<String, Object> maxRelations = null;
                            for (Map.Entry<String, Sketch> sketches : prev.getValue().entrySet()) {
                              Map<String, Object> relations = getSimilarity(nomEntries, sketch, sketches.getValue());
                              double similarity = (double) relations.get("similarity");
                              if (maxSimilarDimension == null || similarity > maxSimilarity) {
                                maxSimilarDimension = sketches.getKey();
                                maxSimilarity = similarity;
                                maxRelations = relations;
                              }
                            }
                            if (maxSimilarity > threshold) {
                              similarities.add(
                                  new Similarity(
                                      dataSource + "." + columnName,
                                      ds + "." + maxSimilarDimension,
                                      maxRelations
                                  )
                              );
                            }
                          }
                        }
                        sketches.put(dataSource, sketchMap);
                        return null;
                      }
                    }
                );
                return null;
              }
            }
        );
        return Sequences.simple(similarities);
      }
    };
  }

  // a - b, b - a, a & b, a | b
  private Map<String, Object> getSimilarity(int nomEntries, Sketch source, Sketch target)
  {
    double A = source.getEstimate();
    double B = target.getEstimate();
    double AorB = ThetaOperations.sketchSetOperation(UNION, nomEntries, source, target).getEstimate();
    double AandB = ThetaOperations.sketchSetOperation(INTERSECT, nomEntries, source, target).getEstimate();
    double A_B = ThetaOperations.sketchSetOperation(NOT, nomEntries, source, target).getEstimate();
    double B_A = ThetaOperations.sketchSetOperation(NOT, nomEntries, target, source).getEstimate();

    double similarity = -1;
    if (A >= B) {
      if (A < B * 1.5f) {
        similarity = AandB / AorB;
      } else if (A < B * 8f) {
        similarity = (B - (Math.pow(B_A, 2d))) / B;
      }
    } else {
      if (B < A * 1.5f) {
        similarity = AandB / AorB;
      } else if (B < A * 8f) {
        similarity = (A - (Math.pow(A_B, 2d))) / A;
      }
    }
    return ImmutableMap.<String, Object>builder()
                       .put("similarity", Math.max(-1, similarity))
                       .put("A", (int) A)
                       .put("B", (int) B)
                       .put("A or B", (int) AorB)
                       .put("A and B", (int) AandB)
                       .put("A-B", (int) A_B)
                       .put("B-A", (int) B_A)
                       .build();
  }

  @Override
  public String toString()
  {
    return "SimilarityProcessingOperator{" +
           "threshold=" + threshold +
           ", dataSourceSet=" + dataSourceSet +
           '}';
  }
}
