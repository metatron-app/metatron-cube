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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.MathExprFilter;

import java.util.List;
import java.util.Map;

/**
 * for test
 */
@JsonTypeName("hash.partitioned")
public class HashPartitionedQuery extends AbstractIteratingQuery<Object[], Object[]> implements Query.ArrayOutput
{
  private final String hashColumn;
  private final int numPartition;

  public HashPartitionedQuery(
      @JsonProperty("query") Query.ArrayOutput query,
      @JsonProperty("hashColumn") String hashColumn,
      @JsonProperty("numPartition") int numPartition,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(query, context);
    this.hashColumn = Preconditions.checkNotNull(hashColumn);
    this.numPartition = numPartition;
    Preconditions.checkArgument(numPartition > 0);
    Preconditions.checkArgument(query instanceof FilterSupport);
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return ((Query.ArrayOutput) query).estimatedOutputColumns();
  }

  @Override
  protected Query<Object[]> newInstance(Query<Object[]> query, Map<String, Object> context)
  {
    return new HashPartitionedQuery((Query.ArrayOutput) query, hashColumn, numPartition, context);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Pair<Sequence<Object[]>, Query<Object[]>> next(Sequence<Object[]> sequence, Query<Object[]> prev)
  {
    if (index < numPartition) {
      return Pair.of(
          sequence,
          ((FilterSupport) query).withFilter(DimFilters.and(
              BaseQuery.getDimFilter(query),
              new MathExprFilter(String.format("abs(hash(%s)) %% %d == %d", hashColumn, numPartition, index++))
          ))
      );
    }
    return Pair.of(sequence, null);
  }
}
