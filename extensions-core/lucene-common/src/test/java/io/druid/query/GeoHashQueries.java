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

import io.druid.data.input.Row;
import io.druid.math.expr.Parser;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.LuceneTestRunner;
import org.geohex.geohex4j.GeoHexFunctions;
import org.junit.Test;

import java.util.List;

public abstract class GeoHashQueries extends LuceneTestRunner
{
  static {
    Parser.register(GeoHashFunctions.class);
    Parser.register(GeoHexFunctions.class);
    Parser.register(H3Functions.class);
  }

  @Test
  public void testGbyOnGeoHash()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.lat", "gis.lon", "count"};
    GroupByQuery.Builder builder = (GroupByQuery.Builder) new GroupByQuery.Builder()
        .dataSource("estate")
        .virtualColumns(new ExprVirtualColumn("to_geohash(gis.lat, gis.lon, 5)", "geohash"))
        .setDimensions(DefaultDimensionSpec.of("geohash"))
        .aggregators(
            CountAggregatorFactory.of("count"),
            RelayAggregatorFactory.first("gu", null),
            RelayAggregatorFactory.first("amt", null),
            RelayAggregatorFactory.first("py", null),
            RelayAggregatorFactory.first("gis.lat", null),
            RelayAggregatorFactory.first("gis.lon", null)
        )
        .limit(15);

    List<Row> expected = createExpectedRows(
        columns,
        array("구로구", 26000L, 84.99, 37.495831, 126.8175835, 12L),
        array("구로구", 17000L, 59.57, 37.4836299, 126.8511212, 15L),
        array("관악구", 13500L, 46.72, 37.4842444, 126.9038832, 141L),
        array("강서구", 28000L, 69.24, 37.5289234, 126.8521217, 313L),
        array("구로구", 14800L, 44.37, 37.4875329, 126.8842305, 433L),
        array("강서구", 38500L, 59.96, 37.5609216, 126.807947, 98L),
        array("강서구", 40000L, 101.86, 37.580799, 126.8129585, 42L),
        array("강서구", 18000L, 34.44, 37.5698458, 126.8471212, 365L),
        array("강서구", 47000L, 84.77, 37.5533144, 126.8736465, 191L),
        array("강서구", 45000L, 84.95, 37.5749051, 126.8378422, 3L),
        array("마포구", 42000L, 49.9, 37.5745284, 126.8910073, 79L),
        array("관악구", 41000L, 80.7, 37.4750472, 126.9521016, 100L),
        array("관악구", 45000L, 95.91, 37.4748687, 126.9716606, 42L),
        array("관악구", 15500L, 19.63, 37.4867225, 126.9474462, 295L),
        array("관악구", 39300L, 59.58, 37.487304, 126.9608926, 181L)
    );
    validate(columns, expected, runQuery(builder.build()));
  }

  @Test
  public void testGbyOnGeoHex()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.lat", "gis.lon", "count"};
    GroupByQuery.Builder builder = (GroupByQuery.Builder) new GroupByQuery.Builder()
        .dataSource("estate")
        .virtualColumns(new ExprVirtualColumn("to_geohex(gis.lat, gis.lon, 5)", "geohex"))
        .setDimensions(DefaultDimensionSpec.of("geohex"))
        .aggregators(
            new CountAggregatorFactory("count"),
            new RelayAggregatorFactory("gu", "gu", "string", "FIRST"),
            new RelayAggregatorFactory("amt", "amt", "double", "FIRST"),
            new RelayAggregatorFactory("py", "py", "double", "FIRST"),
            new RelayAggregatorFactory("gis.lat", "gis.lat", "double", "FIRST"),
            new RelayAggregatorFactory("gis.lon", "gis.lon", "double", "FIRST")
        )
        .limit(15);

    List<Row> expected = createExpectedRows(
        columns,
        array("송파구", 56500L, 59.96, 37.4771295, 127.1302374, 13L),
        array("금천구", 39900L, 59.82, 37.4458596, 126.9007, 12L),
        array("구로구", 17000L, 59.57, 37.4836299, 126.8511212, 7L),
        array("관악구", 45000L, 95.91, 37.4748687, 126.9716606, 55L),
        array("관악구", 15500L, 19.63, 37.4867225, 126.9474462, 848L),
        array("강서구", 40500L, 84.99, 37.5516124, 126.8391742, 651L),
        array("강남구", 105000L, 96.98, 37.4844613, 127.0535707, 444L),
        array("강남구", 70000L, 84.99, 37.5066979, 127.0290002, 496L),
        array("강서구", 18300L, 34.44, 37.5587227, 126.863547, 530L),
        array("강서구", 18000L, 34.44, 37.5698458, 126.8471212, 244L),
        array("강동구", 43700L, 59.72, 37.5597131, 127.1731354, 326L),
        array("강남구", 122000L, 121.48, 37.5235176, 127.0500382, 505L),
        array("강북구", 32100L, 59.96, 37.6151287, 127.0357365, 474L),
        array("강동구", 44500L, 84.53, 37.5606354, 127.1810908, 27L),
        array("강북구", 22400L, 59.42, 37.6262556, 127.0305638, 904L)
    );
    validate(columns, expected, runQuery(builder.build()));
  }

  @Test
  public void testGbyOnH3()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.lat", "gis.lon", "count"};
    GroupByQuery.Builder builder = (GroupByQuery.Builder) new GroupByQuery.Builder()
        .dataSource("estate")
        .virtualColumns(new ExprVirtualColumn("to_h3(gis.lat, gis.lon, 5)", "h3"))
        .setDimensions(DefaultDimensionSpec.of("h3"))
        .aggregators(
            CountAggregatorFactory.of("count"),
            RelayAggregatorFactory.first("gu", null),
            RelayAggregatorFactory.first("amt", null),
            RelayAggregatorFactory.first("py", null),
            RelayAggregatorFactory.first("gis.lat", null),
            RelayAggregatorFactory.first("gis.lon", null)
        )
        .limit(15);

    List<Row> expected = createExpectedRows(
        columns,
        array("관악구", 45000L, 95.91, 37.4748687, 126.9716606, 263L),
        array("강서구", 18000L, 34.44, 37.5698458, 126.8471212, 138L),
        array("강서구", 38400L, 59.76, 37.5608928, 126.8569328, 797L),
        array("강동구", 39300L, 58.68, 37.553258, 127.1251915, 1933L),
        array("강남구", 105000L, 96.98, 37.4844613, 127.0535707, 792L),
        array("강동구", 44500L, 84.53, 37.5606354, 127.1810908, 342L),
        array("강북구", 43500L, 136.75, 37.6455376, 127.0080999, 43L),
        array("노원구", 48000L, 114.92, 37.6819565, 127.0564055, 38L),
        array("강서구", 23000L, 39.6, 37.5632084, 126.8592004, 1516L)
    );
    validate(columns, expected, runQuery(builder.build()));
  }
}
