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

package io.druid.query;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.data.ConstantQuery;
import io.druid.data.GeoToolsFunctions;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.math.expr.Parser;
import io.druid.query.filter.LuceneLatLonPolygonFilter;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.segment.TestHelper;
import io.druid.segment.lucene.ShapeIndexingStrategy;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;

import java.util.List;
import java.util.concurrent.Executors;

public class GeoToolsTestHelper extends TestHelper
{
  static final TestQuerySegmentWalker segmentWalker;

  static {
    Parser.register(GeomFunctions.class);
    Parser.register(GeoToolsFunctions.class);

    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(ShapeIndexingStrategy.class);
    mapper.registerSubtypes(LuceneLatLonPolygonFilter.class);
    mapper.registerSubtypes(LuceneSpatialFilter.class);
    mapper.registerSubtypes(ConstantQuery.class);
    mapper.registerSubtypes(GeoBoundaryFilterQuery.class);

    TestQuerySegmentWalker walker = estateWalker.duplicate()
                                                .withObjectMapper(mapper)
                                                .withExecutor(Executors.newWorkStealingPool(2));

    walker.addIndex("seoul_roads", "seoul_roads_schema.json", "seoul_roads.tsv", true);

    // for toMap post processor
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(QueryToolChestWarehouse.class, segmentWalker = walker));
  }

  public <T> List<T> runQuery(Query<T> query)
  {
    return Sequences.toList(query.run(segmentWalker(), Maps.newHashMap()));
  }

  protected TestQuerySegmentWalker segmentWalker()
  {
    return segmentWalker;
  }
}
