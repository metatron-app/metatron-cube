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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.ValueDesc;
import io.druid.segment.bitmap.BitSetInvertedIndexingSpec;
import io.druid.segment.column.Column;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "histogram", value = HistogramIndexingSpec.class),
    @JsonSubTypes.Type(name = "bsb", value = BitSlicedBitmapSpec.class),
    @JsonSubTypes.Type(name = "bitsetInverted", value = BitSetInvertedIndexingSpec.class),
    @JsonSubTypes.Type(name = "list", value = ListIndexingSpec.class),
})
public interface SecondaryIndexingSpec
{
  MetricColumnSerializer serializer(String columnName, ValueDesc type, Iterable<Object> values);

  /**
   * Build the column by physically merging the secondary indexes of {@code sources} (one per segment being
   * merged, in merged-row order) rather than re-indexing from per-row values. Required for index-only columns,
   * whose base values are gone and so cannot be replayed. Returns {@code null} when the sources carry no such
   * index (e.g. fresh ingestion), letting the caller fall back to {@link #serializer}.
   */
  default MetricColumnSerializer merger(String columnName, ValueDesc type, List<Column> sources) {return null;}

  default Map<String, String> descriptor(String column) {return null;}

  /**
   * If true, the base value column is NOT materialized — only the secondary index is written. The
   * column becomes search-only (e.g. lucene.query works; SELECT/group-by of the value does not).
   * Saves the per-row value storage entirely (vs. storing the raw text just to index it).
   */
  default boolean isIndexOnly() {return false;}
}
