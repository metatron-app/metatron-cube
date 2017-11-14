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

package io.druid.query.groupby;

import com.google.common.collect.Maps;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.AllGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class GroupByBinaryFnV2 implements BinaryFn<Row, Row, Row>
{
  private final GroupByQuery query;
  private final boolean allGranularity;
  private final String[] dimensionNames;
  private final AggregatorFactory[] aggregatorFactories;

  public GroupByBinaryFnV2(GroupByQuery query)
  {
    this.query = query;
    this.allGranularity = query.getGranularity() instanceof AllGranularity;
    List<DimensionSpec> dimensions = query.getDimensions();
    this.dimensionNames = new String[dimensions.size()];
    for (int i = 0; i < dimensionNames.length; i++) {
      dimensionNames[i] = dimensions.get(i).getOutputName();
    }
    List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
    this.aggregatorFactories = new AggregatorFactory[aggregatorSpecs.size()];
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregatorFactories[i] = aggregatorSpecs.get(i);
    }
  }

  @Override
  public Row apply(final Row arg1, final Row arg2)
  {
    if (arg1 == null) {
      return arg2;
    } else if (arg2 == null) {
      return arg1;
    }

    final Map<String, Object> newMap = Maps.newHashMapWithExpectedSize(
        dimensionNames.length + aggregatorFactories.length
    );

    // Add dimensions
    for (String dimensionName : dimensionNames) {
      newMap.put(dimensionName, arg1.getRaw(dimensionName));
    }

    // Add aggregations
    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      final String name = aggregatorFactory.getName();
      newMap.put(name, aggregatorFactory.combine(arg1.getRaw(name), arg2.getRaw(name)));
    }

    return new MapBasedRow(adjustTimestamp(arg1), newMap);
  }

  private DateTime adjustTimestamp(final Row row)
  {
    if (allGranularity) {
      return row.getTimestamp();
    } else {
      return query.getGranularity().bucketStart(row.getTimestamp());
    }
  }
}
