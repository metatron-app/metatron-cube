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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.segment.StringArray;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@JsonTypeName("explode")
public class RowExploder implements PostProcessingOperator.LogProvider, RowSignature.Evolving
{
  private final List<String> columns;
  private final Map<String, Integer> exploder;
  private final StringArray.IntMap exploderM;

  @JsonCreator
  public RowExploder(
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("exploder") Map<String, Integer> exploder,
      @JsonProperty("exploderM") StringArray.IntMap exploderM
  )
  {
    this.columns = columns;
    this.exploder = exploder;
    this.exploderM = exploderM;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Integer> getExploder()
  {
    return exploder;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public StringArray.IntMap getExploderM()
  {
    return exploderM;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    if (exploder != null) {
      return new QueryRunner()
      {
        @Override
        public Sequence run(Query query, Map response)
        {
          Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "?? %s", query);
          Query.ArrayOutputSupport arrayQuery = (Query.ArrayOutputSupport) query;   // currently stream query only

          final Sequence<Object[]> sequence = QueryRunners.runArray(arrayQuery, baseRunner);
          final int index = sequence.columns().indexOf(columns.get(0));
          return Sequences.explode(sequence, v -> {
            final int exploded = exploder.getOrDefault(Objects.toString(v[index], ""), 1);
            if (exploded == 1) {
              return Sequences.simple(v);
            }
            return Sequences.simple(() -> new Iterator<Object[]>()
            {
              private int index;

              @Override
              public boolean hasNext()
              {
                return index < exploded;
              }

              @Override
              public Object[] next()
              {
                return index++ == 0 ? v : Arrays.copyOf(v, v.length);
              }
            });
          });
        }
      };
    }
    return new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map response)
      {
        Preconditions.checkArgument(query instanceof Query.ArrayOutputSupport, "?? %s", query);
        Query.ArrayOutputSupport arrayQuery = (Query.ArrayOutputSupport) query;   // currently stream query only

        final Sequence<Object[]> sequence = QueryRunners.runArray(arrayQuery, baseRunner);
        final int[] indices = GuavaUtils.indexOf(sequence.columns(), columns);

        final String[] array = new String[indices.length];
        final StringArray key = StringArray.of(array);
        return Sequences.explode(sequence, v -> {
          for (int i = 0; i < indices.length; i++) {
            array[i] = Objects.toString(v[indices[i]], "");
          }
          final int exploded = exploderM.getOrDefault(key, 1);
          if (exploded == 1) {
            return Sequences.simple(v);
          }
          return Sequences.simple(() -> new Iterator<Object[]>()
          {
            private int index;

            @Override
            public boolean hasNext()
            {
              return index < exploded;
            }

            @Override
            public Object[] next()
            {
              return index++ == 0 ? v : Arrays.copyOf(v, v.length);
            }
          });
        });
      }
    };
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    return schema;
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    return schema;
  }

  @Override
  public PostProcessingOperator forLog()
  {
    return new RowExploder(columns, null, null);
  }

  @Override
  public String toString()
  {
    return "RowExplodeProcessor{columns=" + columns + '}';
  }
}
