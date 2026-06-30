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

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.data.ConstantQuery;
import io.druid.data.EnvelopeAggregatorFactory;
import io.druid.data.output.GeoJsonDecorator;
import io.druid.initialization.DruidModule;
import io.druid.query.GeoHashFunctions;
import io.druid.query.GeomFunctions;
import io.druid.query.GeometryDeserializer;
import io.druid.query.GeometrySerializer;
import io.druid.query.H3Functions;
import io.druid.query.aggregation.GeomCollectPointAggregatorFactory;
import io.druid.query.aggregation.GeomUnionAggregatorFactory;
import io.druid.query.filter.H3PointDistanceFilter;
import org.geohex.geohex4j.GeoHexFunctions;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class GeometryExtensionModule implements DruidModule
{
  public SimpleModule getModule(boolean lucene7)
  {
    // NOTE: must be a UNIQUE module name. Jackson's ObjectMapper.registerModule skips a module whose
    // type id (the SimpleModule name) was already registered (IGNORE_DUPLICATE_MODULE_REGISTRATIONS,
    // on by default). This module loads before lucene, so sharing the name "lucene-extension" with
    // LuceneCommonExtensionModule caused the latter to be silently skipped -> lucene.query et al. unregistered.
    SimpleModule module = new SimpleModule("geometry-extension")
        .registerSubtypes(GeoHashFunctions.class)
        .registerSubtypes(GeoHexFunctions.class)
        .registerSubtypes(GeomFunctions.class)
        .registerSubtypes(GeoJsonDecorator.class)
        .registerSubtypes(GeomUnionAggregatorFactory.class)
        .registerSubtypes(GeomCollectPointAggregatorFactory.class)
        .addSerializer(Geometry.class, new GeometrySerializer())
        .addDeserializer(Geometry.class, new GeometryDeserializer());

    module.registerSubtypes(H3Functions.class)
          .registerSubtypes(H3IndexingSpec.class)
          .registerSubtypes(H3IndexingSpec.SerDe.class)
          .registerSubtypes(H3PointDistanceFilter.class);

    // moved from geotools extension
    module.registerSubtypes(ConstantQuery.class)
          .registerSubtypes(EnvelopeAggregatorFactory.class);

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
    // SQL/Calcite bindings isolated in GeometrySqlBindings (keeps this module free of io.druid.sql.*)
    GeometrySqlBindings.register(binder);
  }
}
