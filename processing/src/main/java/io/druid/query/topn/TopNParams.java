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

package io.druid.query.topn;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;

import java.util.List;

/**
 */
public class TopNParams
{
  private final DimensionSelector dimSelector;
  private final Cursor cursor;
  private final ColumnSelectorFactory factory;
  private final int cardinality;
  private final int numValuesPerPass;
  private final Aggregator[] aggregators;

  protected TopNParams(
      DimensionSelector dimSelector,
      List<AggregatorFactory> factories,
      Cursor cursor,
      int cardinality,
      int numValuesPerPass
  )
  {
    this.dimSelector = dimSelector;
    this.cursor = cursor;
    this.cardinality = cardinality;
    this.numValuesPerPass = numValuesPerPass;
    this.factory = BaseTopNAlgorithm.wrap(cursor, dimSelector);
    this.aggregators = Aggregators.makeAggregators(factories, factory);
  }

  public DimensionSelector getDimSelector()
  {
    return dimSelector;
  }

  public Aggregator[] getAggregators()
  {
    return aggregators;
  }

  public Cursor getCursor()
  {
    return cursor;
  }

  public ColumnSelectorFactory getFactory()
  {
    return factory;
  }

  public int getCardinality()
  {
    return cardinality;
  }

  public int getNumValuesPerPass()
  {
    return numValuesPerPass;
  }
}
