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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.math.expr.Parser;
import io.druid.query.GeomFunctions;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.segment.lucene.Lucene9ExtensionModule;
import io.druid.segment.lucene.LuceneCommonExtensionModule;
import io.druid.segment.lucene.LuceneKnnVectorConversion;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;

import java.util.concurrent.Executors;

public class Lucene9TestHelper extends TestHelper
{
  public static final TestQuerySegmentWalker segmentWalker;

  static {
    Parser.register(GeomFunctions.class);

    ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new LuceneCommonExtensionModule().getJacksonModules()) {
      mapper.registerModule(module);
    }
    for (Module module : new Lucene9ExtensionModule().getJacksonModules()) {
      mapper.registerModule(module);
    }
    IndexIO indexIO = new IndexIO(mapper);
    IndexMergerV9 indexMerger = new IndexMergerV9(mapper, indexIO);

    segmentWalker = newWalker().withObjectMapper(mapper)
                               .withExecutor(Executors.newWorkStealingPool(2));

    segmentWalker.addIndex("estate", "estate9_schema.json", "estate.csv", true);
    segmentWalker.addIndex("estate_incremental", "estate9_schema.json", "estate.csv", false);
    segmentWalker.addIndex("chameleon", "chameleon_schema.json", "chameleon.ds.txt", true);

    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(IndexIO.class, indexIO)
            .addValue(IndexMerger.class, indexMerger)
            .addValue(ObjectMapper.class, mapper)
            .addValue(QueryToolChestWarehouse.class, segmentWalker)
    );

    CalciteTests.register(new LuceneKnnVectorConversion());
  }
}
