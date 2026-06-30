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

package io.druid.segment.lucene;

import com.google.inject.Binder;
import io.druid.sql.calcite.planner.LuceneNearestFilterConversion;
import io.druid.sql.calcite.planner.LuceneQueryFilterConversion;
import io.druid.sql.calcite.planner.LuceneShapeFilterConversion;
import io.druid.sql.guice.SqlBindings;

/**
 * SQL (Calcite) filter conversions for the lucene-common filters.
 *
 * Isolated here so {@link LuceneCommonExtensionModule} carries no reference to {@code io.druid.sql.*}:
 * the module class (and its Jackson subtype registration) can then be loaded and used without
 * druid-sql / Calcite on the classpath (e.g. by an embedded segment writer). These bindings are pulled
 * in only when {@code register} is actually invoked — i.e. from the module's Guice {@code configure}
 * on a full node where druid-sql is present.
 */
public final class LuceneCommonSqlBindings
{
  private LuceneCommonSqlBindings() {}

  public static void register(Binder binder)
  {
    SqlBindings.addFilterConversion(binder, LuceneQueryFilterConversion.class);
    SqlBindings.addFilterConversion(binder, LuceneNearestFilterConversion.class);
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_EQUALS", SpatialOperations.EQUALTO));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_WITHIN", SpatialOperations.COVEREDBY));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_CONTAINS", SpatialOperations.COVERS));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_INTERSECTS", SpatialOperations.INTERSECTS));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_OVERLAPS", SpatialOperations.OVERLAPS));
  }
}
