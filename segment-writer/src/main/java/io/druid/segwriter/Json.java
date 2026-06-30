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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.initialization.DruidModule;
import io.druid.jackson.DefaultObjectMapper;

import java.util.ServiceLoader;

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
   * Mapper that also knows the extension Jackson subtypes. Use it for anything touching
   * Lucene-indexed segments: deserializing a {@code secondaryIndexing} spec into SecondaryIndexingSpec,
   * and reading/writing column descriptors via IndexIO/IndexMergerV9 (the column part serde subtype
   * id {@code "lucene10"} must be resolvable on read-back).
   *
   * We discover the DruidModules bundled in this (shaded) jar via ServiceLoader and register their
   * Jackson subtypes — the lean equivalent of what the cluster does through Guice. This works without
   * druid-sql/Calcite because the extension modules' getJacksonModules() no longer reference any
   * io.druid.sql.* (those bindings were moved to separate *SqlBindings helpers invoked only from
   * Guice configure(), which the writer never calls).
   */
  public static ObjectMapper indexMapper()
  {
    return INDEX_MAPPER;
  }

  private static ObjectMapper createIndexMapper()
  {
    final ObjectMapper m = MAPPER.copy();   // copy() does NOT re-run the ComplexMetrics registration
    for (DruidModule dm : ServiceLoader.load(DruidModule.class, Json.class.getClassLoader())) {
      for (Module mod : dm.getJacksonModules()) {
        m.registerModule(mod);
      }
    }
    return m;
  }
}
