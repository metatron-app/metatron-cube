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

import io.druid.segment.Lucene8TestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;

public class TestShapeQuery extends ShapeQueries
{
  private static final TestQuerySegmentWalker segmentWalker;

  static {
    segmentWalker = Lucene8TestHelper.segmentWalker.duplicate();
    segmentWalker.addIndex("seoul_roads", "seoul_roads8_schema.json", "seoul_roads.tsv", true);
    segmentWalker.addIndex("seoul_roads_incremental", "seoul_roads8_schema.json", "seoul_roads.tsv", false);
    segmentWalker.addIndex("world_border", "world8_schema.json", "world.csv", true);
  }

  @Override
  protected TestQuerySegmentWalker segmentWalker()
  {
    return segmentWalker;
  }
}
