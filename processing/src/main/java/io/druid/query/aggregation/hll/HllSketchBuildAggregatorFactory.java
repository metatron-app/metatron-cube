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

package io.druid.query.aggregation.hll;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;

/**
 * This aggregator factory is for building sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 * @author Alexander Saydakov
 */
public class HllSketchBuildAggregatorFactory extends HllSketchAggregatorFactory
{
  @JsonCreator
  public HllSketchBuildAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("lgK") @Nullable final Integer lgK,
      @JsonProperty("tgtHllType") @Nullable final String tgtHllType)
  {
    super(name, fieldName, lgK, tgtHllType);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of(HllSketchModule.BUILD_TYPE_NAME);
  }

  @Override
  protected byte getCacheTypeId()
  {
    return HllSketchModule.HLL_SKETCH_BUILD_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ObjectColumnSelector<Object> selector = columnSelectorFactory.makeObjectColumnSelector(getFieldName());
    return new HllSketchBuildAggregator(selector, getLgK(), TgtHllType.valueOf(getTgtHllType()));
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ObjectColumnSelector<Object> selector = columnSelectorFactory.makeObjectColumnSelector(getFieldName());
    return new HllSketchBuildBufferAggregator(
        selector,
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getMaxIntermediateSize()
    );
  }

  /**
   * For the HLL_4 sketch type, this value can be exceeded slightly in extremely rare cases.
   * The sketch will request on-heap memory and move there. It is handled in HllSketchBuildBufferAggregator.
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return HllSketch.getMaxUpdatableSerializationBytes(getLgK(), TgtHllType.valueOf(getTgtHllType()));
  }
}
