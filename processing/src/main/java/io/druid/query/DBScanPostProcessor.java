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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.query.kmeans.Centroid;
import io.druid.query.kmeans.KMeansQuery;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@JsonTypeName("dbScan")
public class DBScanPostProcessor extends PostProcessingOperator.ReturnsArray implements RowSignature.Evolving
{
  private final double eps;
  private final int minPts;

  @JsonCreator
  public DBScanPostProcessor(
      @JsonProperty("eps") double eps,
      @JsonProperty("minPts") int minPts
  )
  {
    this.eps = eps;
    this.minPts = minPts;
  }

  @JsonProperty
  public double getEps()
  {
    return eps;
  }

  @JsonProperty
  public int getMinPts()
  {
    return minPts;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner runner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        Sequence sequence = runner.run(query, responseContext);
        List<Centroid> centroids;
        if (query instanceof KMeansQuery) {
          centroids = Sequences.toList(sequence);
        } else {
          centroids = Sequences.toList(Sequences.map(
              sequence,
              new Function<Object[], Centroid>()
              {
                @Override
                public Centroid apply(Object[] input)
                {
                  if (input.length == 1) {
                    return new Centroid((double[]) input[0]);
                  }
                  final double[] point = new double[input.length];
                  for (int i = 0; i < input.length; i++) {
                    point[i] = ((Number) input[i]).doubleValue();
                  }
                  return new Centroid(point);
                }
              }
          ));
        }
        List<String> columns = GuavaUtils.concat(sequence.columns(), "tag");
        return Sequences.once(columns, Iterators.concat(Iterators.transform(
            new DBScan(eps, minPts).cluster(centroids), new Function<List<Centroid>, Iterator<Object[]>>()
            {
              private int index;

              @Override
              public Iterator<Object[]> apply(List<Centroid> input)
              {
                return Iterators.transform(input.iterator(), new Tag(index++));
              }
            }
        )));
      }
    };
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    return schema == null ? null : GuavaUtils.concat(schema, "tag");
  }

  @Override
  public RowSignature evolve(Query query, RowSignature schema)
  {
    return schema == null ? null : schema.append("tag", ValueDesc.LONG);
  }

  private static class Tag implements Function<Centroid, Object[]>
  {
    private final int index;

    private Tag(int index) {this.index = index;}

    @Override
    public Object[] apply(Centroid input)
    {
      final double[] point = input.getPoint();
      final Object[] array = new Object[point.length + 1];
      for (int i = 0; i < point.length; i++) {
        array[i] = point[i];
      }
      array[point.length] = index;
      return array;
    }
  }
}
