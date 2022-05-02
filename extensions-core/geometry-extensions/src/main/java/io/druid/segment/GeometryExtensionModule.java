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
import io.druid.data.output.GeoJsonFormatter;
import io.druid.initialization.DruidModule;
import io.druid.query.GeoHashFunctions;
import io.druid.query.GeomFunctions;
import io.druid.query.GeometryDeserializer;
import io.druid.query.GeometrySerializer;
import io.druid.query.H3Functions;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.GeomCollectPointAggregatorFactory;
import io.druid.query.aggregation.GeomUnionAggregatorFactory;
import io.druid.sql.guice.SqlBindings;
import org.geohex.geohex4j.GeoHexFunctions;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public class GeometryExtensionModule implements DruidModule
{
  public SimpleModule getModule(boolean lucene7)
  {
    SimpleModule module = new SimpleModule("lucene-extension")
        .registerSubtypes(GeoHashFunctions.class)
        .registerSubtypes(H3Functions.class)
        .registerSubtypes(GeoHexFunctions.class)
        .registerSubtypes(GeomFunctions.class)
        .registerSubtypes(GeoJsonDecorator.class)
        .registerSubtypes(GeoJsonFormatter.class)
        .registerSubtypes(GeomUnionAggregatorFactory.class)
        .registerSubtypes(GeomCollectPointAggregatorFactory.class)
        .addSerializer(Geometry.class, new GeometrySerializer())
        .addDeserializer(Geometry.class, new GeometryDeserializer());

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
    SqlBindings.addAggregator(
        binder, AggregatorFactory.bundleSQL(new GeomUnionAggregatorFactory("<name>", "<columnName>"))
    );
    SqlBindings.addAggregator(
        binder, AggregatorFactory.bundleSQL(new GeomCollectPointAggregatorFactory("<name>", "<columnName>"))
    );
  }
}
