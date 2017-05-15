/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.kurtosis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.stats.DruidStatsModule;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class KurtosisTestHelper extends QueryRunnerTestHelper
{
  static {
    DruidStatsModule module = new DruidStatsModule();
    module.configure(null);
  }

  public static final String indexKurtosisMetric = "index_kurtosis";

  public static final KurtosisAggregatorFactory indexKurtosisAggr = new KurtosisAggregatorFactory(
      indexKurtosisMetric,
      "index",
      null,
      "float"
  );

  public static final List<AggregatorFactory> commonPlusCorrAggregators = Arrays.asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques,
      indexKurtosisAggr
  );

  public static class RowBuilder
  {
    private final String[] names;
    private final List<Row> rows = Lists.newArrayList();

    public RowBuilder(String[] names)
    {
      this.names = names;
    }

    public RowBuilder add(final String timestamp, Object... values)
    {
      rows.add(build(timestamp, values));
      return this;
    }

    public List<Row> build()
    {
      try {
        return Lists.newArrayList(rows);
      }
      finally {
        rows.clear();
      }
    }

    public Row build(final String timestamp, Object... values)
    {
      Map<String, Object> theVals = Maps.newHashMap();
      for (int i = 0; i < names.length; i++) {
        theVals.put(names[i], values[i]);
      }
      DateTime ts = new DateTime(timestamp);
      return new MapBasedRow(ts, theVals);
    }
  }
}
