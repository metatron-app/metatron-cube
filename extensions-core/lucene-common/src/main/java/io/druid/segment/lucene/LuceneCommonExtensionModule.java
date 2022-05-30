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

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.query.ChoroplethMapQuery;
import io.druid.query.GeoBoundaryFilterQuery;
import io.druid.query.filter.LuceneGeoJsonPolygonFilter;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.filter.LuceneNearestFilter;
import io.druid.query.filter.LucenePointFilter;
import io.druid.query.filter.LuceneQueryFilter;
import io.druid.query.filter.LuceneShapeFilter;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.query.filter.RegexFSTFilter;
import io.druid.sql.calcite.planner.LuceneNearestFilterConversion;
import io.druid.sql.calcite.planner.LuceneQueryFilterConversion;
import io.druid.sql.calcite.planner.LuceneShapeFilterConversion;
import io.druid.sql.guice.SqlBindings;

import java.util.List;

public class LuceneCommonExtensionModule implements DruidModule
{
  public SimpleModule getModule(boolean lucene7)
  {
    SimpleModule module = new SimpleModule("lucene-extension")
        .registerSubtypes(TextIndexingStrategy.class)
        .registerSubtypes(LatLonPointIndexingStrategy.class)
        .registerSubtypes(ShapeIndexingStrategy.class)
        .registerSubtypes(SpatialIndexingStrategy.class)
        .registerSubtypes(LatLonShapeIndexingStrategy.class)
        .registerSubtypes(JsonIndexingStrategy.class)
        .registerSubtypes(LuceneQueryFilter.class)
        .registerSubtypes(LucenePointFilter.class)
        .registerSubtypes(LuceneNearestFilter.class)
        .registerSubtypes(LuceneSpatialFilter.class)
        .registerSubtypes(LuceneLatLonPolygonFilter.class)
        .registerSubtypes(LuceneShapeFilter.class)
        .registerSubtypes(RegexFSTFilter.class)
        .registerSubtypes(LuceneGeoJsonPolygonFilter.class)
        .registerSubtypes(ChoroplethMapQuery.class)
        .registerSubtypes(GeoBoundaryFilterQuery.class)
        .registerSubtypes(FSTBuilder.class);

    if (lucene7) {
      module.registerSubtypes(LuceneIndexingSpec.class)
            .registerSubtypes(LuceneIndexingSpec.SerDe.class);
    }

    return module;
  }

  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return ImmutableList.of(getModule(true));
  }

  @Override
  public void configure(Binder binder)
  {
    SqlBindings.addFilterConversion(binder, LuceneQueryFilterConversion.class);
    SqlBindings.addFilterConversion(binder, LuceneNearestFilterConversion.class);
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_EQUALS", SpatialOperations.EQUALTO));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_WITHIN", SpatialOperations.COVEREDBY));
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_CONTAINS", SpatialOperations.COVERS));
    SqlBindings.addFilterConversion(
        binder,
        LuceneShapeFilterConversion.of("ST_INTERSECTS", SpatialOperations.INTERSECTS)
    );
    SqlBindings.addFilterConversion(binder, LuceneShapeFilterConversion.of("ST_OVERLAPS", SpatialOperations.OVERLAPS));
  }
}
