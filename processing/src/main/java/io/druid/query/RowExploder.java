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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 */
@JsonTypeName("explode")
public class RowExploder implements PostProcessingOperator.LogProvider, RowSignature.Evolving
{
  public static RowExploder of(String column, Map<String, Integer> mapping, List<String> outputColumns)
  {
    return new RowExploder(Arrays.asList(column), mapping, null, outputColumns);
  }

  public static RowExploder of(List<String> columns, StringArray.ToIntMap mapping, List<String> outputColumns)
  {
    return new RowExploder(columns, null, mapping, outputColumns);
  }

  private final List<String> keys;
  private final Map<String, Integer> exploder;
  private final StringArray.ToIntMap exploderM;
  private final List<String> outputColumns;

  @JsonCreator
  public RowExploder(
      @JsonProperty("keys") List<String> keys,
      @JsonProperty("exploder") Map<String, Integer> exploder,
      @JsonProperty("exploderM") StringArray.ToIntMap exploderM,
      @JsonProperty("outputColumns") List<String> outputColumns
  )
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
    this.exploder = exploder;
    this.exploderM = exploderM;
    this.outputColumns = outputColumns;
  }

  @JsonProperty
  public List<String> getKeys()
  {
    return keys;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Integer> getExploder()
  {
    return exploder;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public StringArray.ToIntMap getExploderM()
  {
    return exploderM;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getOutputColumns()
  {
    return outputColumns;
  }

  @Override
  public QueryRunner postProcess(QueryRunner runner)
  {
    // only works with stream query, for now
    return (query, response) -> explode(QueryRunners.runArray(query, runner));
  }

  private Sequence<Object[]> explode(Sequence<Object[]> sequence)
  {
    return exploderM == null ? _explode(sequence) : _explodeM(sequence);
  }

  private Sequence<Object[]> _explode(Sequence<Object[]> sequence)
  {
    final List<String> columns = sequence.columns();
    final int index = columns.indexOf(keys.get(0));
    final ToIntFunction<Object[]> repeater = v -> exploder.getOrDefault(Objects.toString(v[index], ""), 1);
    final int[] projection = GuavaUtils.indexOf(columns, outputColumns);
    if (projection == null) {
      return Sequences.explode(sequence, v -> Sequences.repeat(v, repeater.applyAsInt(v)));
    }
    return Sequences.explode(sequence, v -> Sequences.repeat(GuavaUtils.map(v, projection), repeater.applyAsInt(v)));
  }

  private Sequence<Object[]> _explodeM(Sequence<Object[]> sequence)
  {
    final List<String> columns = sequence.columns();
    final int[] indices = GuavaUtils.indexOf(columns, keys);

    final String[] array = new String[indices.length];
    final StringArray key = StringArray.of(array);
    final ToIntFunction<Object[]> repeater = v -> {
      for (int i = 0; i < indices.length; i++) {
        array[i] = Objects.toString(v[indices[i]], "");
      }
      return exploderM.getOrDefault(key, 1);
    };
    final int[] projection = GuavaUtils.indexOf(columns, outputColumns);
    if (projection == null) {
      return Sequences.explode(sequence, v -> Sequences.repeat(v, repeater.applyAsInt(v)));
    }
    return Sequences.explode(sequence, v -> Sequences.repeat(GuavaUtils.map(v, projection), repeater.applyAsInt(v)));
  }

  @Override
  public List<String> evolve(List<String> schema)
  {
    return outputColumns == null ? schema : outputColumns;
  }

  @Override
  public RowSignature evolve(RowSignature schema)
  {
    return outputColumns == null ? schema : schema.retain(outputColumns);
  }

  @Override
  public PostProcessingOperator forLog()
  {
    return new RowExploder(keys, null, null, outputColumns);
  }

  @Override
  public String toString()
  {
    return "RowExplodeProcessor{"
           + "keys=" + keys +
           (outputColumns == null ? "" : ", outputColumns=" + outputColumns) +
           '}';
  }
}
