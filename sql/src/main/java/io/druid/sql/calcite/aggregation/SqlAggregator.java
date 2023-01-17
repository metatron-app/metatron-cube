/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.aggregation;

import io.druid.query.filter.DimFilter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;

/**
 * Bridge between Druid and SQL aggregators.
 */
public interface SqlAggregator
{
  /**
   * Returns the SQL operator corresponding to this aggregation function. Should be a singleton.
   *
   * @return operator
   */
  SqlAggFunction calciteFunction();

  /**
   * Returns a Druid Aggregation corresponding to a SQL {@link AggregateCall}. This method should ignore filters;
   * they will be applied to your aggregator in a later step.
   *
   * @param aggregations       SQL planner context
   * @param predicate
   * @param call        aggregate call object
   * @param outputName                 desired output name of the aggregation
   * @return aggregation, or null if the call cannot be translated
   */
  boolean register(Aggregations aggregations, DimFilter predicate, AggregateCall call, String outputName);
}
