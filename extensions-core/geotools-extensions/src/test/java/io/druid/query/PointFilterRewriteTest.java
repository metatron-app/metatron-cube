/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableMap;
import io.druid.query.filter.LucenePointFilter;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class PointFilterRewriteTest extends GeoToolsTestHelper
{
  @Test
  public void testPointFilter()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.addr"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("estate_incremental")
        .columns(columns)
        .orderBy(OrderByColumnSpec.descending("amt"))
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));

    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
    );
    builder.filters(LucenePointFilter.distance("gis.coord", 37.492927, 127.020779, 400));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    // not supported, yet
//    expected = createExpectedMaps(
//        columns,
//        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
//        new Object[]{"서초구", 82700L, 84.82F, "서초동 1687 유원서초"},
//        new Object[]{"서초구", 81000L, 84.82F, "서초동 1687 유원서초"},
//        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
//        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
//        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
//        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
//    );
//    builder.filters(LucenePointFilter.nearest("gis.coord", 37.492927, 127.020779, 7));
//    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }
}
