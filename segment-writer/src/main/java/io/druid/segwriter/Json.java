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

package io.druid.segwriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.lucene.JsonIndexingStrategy;
import io.druid.segment.lucene.KnnVectorStrategy;
import io.druid.segment.lucene.LatLonPointIndexingStrategy;
import io.druid.segment.lucene.LatLonShapeIndexingStrategy;
import io.druid.segment.lucene.Lucene10FSTSerDe;
import io.druid.segment.lucene.Lucene10IndexingSpec;
import io.druid.segment.lucene.ShapeIndexingStrategy;
import io.druid.segment.lucene.SpatialIndexingStrategy;
import io.druid.segment.lucene.TextIndexingStrategy;

/**
 * One shared Druid ObjectMapper per JVM.
 *
 * DefaultObjectMapper's constructor registers complex-metric serdes in a global static registry
 * (ComplexMetrics), which throws if a type is registered twice. Constructing it more than once in a
 * JVM — e.g. concurrently across Spark executor tasks — fails with "Serde for type [...] already
 * exists". Use this singleton everywhere instead of `new DefaultObjectMapper()`. The static-final
 * init is thread-safe and runs exactly once per classloader.
 */
public final class Json
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final ObjectMapper INDEX_MAPPER = createIndexMapper();

  private Json() {}

  /** Plain Druid mapper. */
  public static ObjectMapper mapper()
  {
    return MAPPER;
  }

  /**
   * Mapper that also knows the Lucene secondary-index Jackson subtypes. Use it for anything touching
   * Lucene-indexed segments: deserializing a {@code secondaryIndexing} spec into SecondaryIndexingSpec,
   * and reading/writing column descriptors via IndexIO/IndexMergerV9 (the column part serde subtype
   * id {@code "lucene10"} must be resolvable on read-back).
   *
   * We register only the indexing subtypes — NOT the extension's full getJacksonModules(), which also
   * pulls query-side filters + SQL/Calcite conversions the writer never uses.
   */
  public static ObjectMapper indexMapper()
  {
    return INDEX_MAPPER;
  }

  private static ObjectMapper createIndexMapper()
  {
    final ObjectMapper m = MAPPER.copy();   // copy() does NOT re-run the ComplexMetrics registration
    m.registerSubtypes(
        // SecondaryIndexingSpec + strategies (parse the secondaryIndexing spec)
        Lucene10IndexingSpec.class,        // "lucene10"
        TextIndexingStrategy.class,        // "text"
        JsonIndexingStrategy.class,        // "json"
        ShapeIndexingStrategy.class,       // "shape"
        SpatialIndexingStrategy.class,     // "spatial"
        LatLonPointIndexingStrategy.class, // "latlon.point"
        LatLonShapeIndexingStrategy.class, // "latlon.shape"
        KnnVectorStrategy.class,           // "knn.vector"
        // ColumnPartSerde subtypes (read back lucene-indexed columns)
        Lucene10IndexingSpec.SerDe.class,  // "lucene10" ColumnPartSerde
        Lucene10FSTSerDe.class
    );
    return m;
  }
}
