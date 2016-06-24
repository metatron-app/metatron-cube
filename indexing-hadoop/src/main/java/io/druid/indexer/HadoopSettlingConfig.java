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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.range.RangeAggregatorFactory;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.List;
import java.util.Map;

public class HadoopSettlingConfig implements SettlingConfig
{
  final private MetadataStorageConnectorConfig config;
  final private String query;
  final private List<String> constColumns;
  final private List<String> regexColumns;
  final private String aggTypeColumn;
  final private String offsetColumn;
  final private String sizeColumn;
  final private String settlingYN;

  private Map<MultiKey, Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>>> settlingMap;
  private boolean getSource = false;
  private String[] dimValues;
  private String[] regexValues;
  private HadoopSettlingMatcherFactory matcherFactory;
  protected String DEFAULT_SETTLING_YN = "settling";

  @JsonCreator
  public HadoopSettlingConfig(
    @JsonProperty(value = "connectorConfig", required = true)
    final MetadataStorageConnectorConfig connectorConfig,
    @JsonProperty(value = "query", required = true)
    final String query,
    @JsonProperty(value = "constColumns", required = true)
    final List<String> constColumns,
    @JsonProperty(value = "regexColumns", required = true)
    final List<String> regexColumns,
    @JsonProperty(value = "typeColumn", required = true)
    final String aggTypeColumn,
    @JsonProperty(value = "offsetColumn", required = true)
    final String offset,
    @JsonProperty(value = "sizeColumn", required = true)
    final String size,
    @JsonProperty(value = "settlingYNColumn")
    final String settlingYN
  )
  {
    this.config = Preconditions.checkNotNull(connectorConfig);
    this.query = Preconditions.checkNotNull(query);
    this.constColumns = Preconditions.checkNotNull(constColumns);
    this.regexColumns = Preconditions.checkNotNull(regexColumns);
    this.aggTypeColumn = Preconditions.checkNotNull(aggTypeColumn);
    this.offsetColumn = Preconditions.checkNotNull(offset);
    this.sizeColumn = Preconditions.checkNotNull(size);
    this.settlingYN = settlingYN == null ? DEFAULT_SETTLING_YN : settlingYN;
    settlingMap = Maps.newHashMap();
    dimValues = new String[constColumns.size()];
    regexValues = new String[regexColumns.size()];
    matcherFactory = new HadoopSettlingMatcherFactory();
  }

  @JsonProperty("connectorConfig")
  public MetadataStorageConnectorConfig getConfig()
  {
    return config;
  }

  @JsonProperty("query")
  public String getQuery()
  {
    return query;
  }

  @JsonProperty("constColumns")
  public List<String> getConstColumns()
  {
    return constColumns;
  }

  @JsonProperty("regexColumns")
  public List<String> getRegexColumns()
  {
    return regexColumns;
  }

  @JsonProperty("typeColumn")
  public String getAggTypeColumn()
  {
    return aggTypeColumn;
  }

  @JsonProperty("offsetColumn")
  public String getOffsetColumn()
  {
    return offsetColumn;
  }

  @JsonProperty("sizeColumn")
  public String getSizeColumn()
  {
    return sizeColumn;
  }

  @JsonProperty("settlingYNColumn")
  public String getSettlingYN()
  {
    return settlingYN;
  }

  @Override
  public void setUp()
  {
    if (!getSource) {
      // connect through the given connector
      final Handle handle = new DBI(
          config.getConnectURI(),
          config.getUser(),
          config.getPassword()
      ).open();

      // fill the Map
      List<Map<String, Object>> results = handle.select(query);
      String[] keys = new String[constColumns.size()];
      String[] regexKeys = new String[regexColumns.size()];
      for (Map<String, Object> row: results)
      {
        int idx = 0;
        for (String column: constColumns)
        {
          keys[idx++] = (String)row.get(column);
        }
        MultiKey key = new MultiKey(keys);
        Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> settlingMatcherMap = settlingMap.get(key);
        if (settlingMatcherMap == null) {
          settlingMatcherMap = Maps.newHashMap();
          settlingMap.put(key, settlingMatcherMap);
        }

        idx = 0;
        for (String column: regexColumns)
        {
          regexKeys[idx++] = (String)row.get(column);
        }
        HadoopSettlingMatcher settlingMatcher = matcherFactory.getSettlingMatcher(regexKeys);
        Map<String, Pair<Integer, Integer>> map = settlingMatcherMap.get(settlingMatcher);
        if (map == null) {
          map = Maps.newHashMap();
          settlingMatcherMap.put(settlingMatcher, map);
        }

        String type = (String)row.get(aggTypeColumn);
        Pair<Integer, Integer> value =
            new Pair<>((int)Float.parseFloat((String)row.get(offsetColumn)), (int)Float.parseFloat((String)row.get(sizeColumn)));
        map.put(type, value);
      }

      handle.close();

      getSource = true;
    }
  }

  @Override
  public boolean applySettling(InputRow row, AggregatorFactory[] org, AggregatorFactory[] applied)
  {
    Preconditions.checkArgument(getSource, "setUp() should be called before the applySettling()");

    Map<String, Pair<Integer, Integer>> mapForRow = getValueMap(row);
    boolean settlingApplied = false;

    if (mapForRow != null)
    {
      // special treat for mean aggregator settling values
      Pair<Integer, Integer> mean = mapForRow.get("ME");
      for (int idx = 0; idx < org.length; idx++)
      {
        String type = getAggCode(org[idx]);
        if (type != null) {
          Pair<Integer, Integer> aggRange = mapForRow.get(type);
          if (aggRange != null) {
            applied[idx] = new RangeAggregatorFactory(org[idx], aggRange.lhs, aggRange.rhs);
            settlingApplied = true;
          } else if (mean != null) {
            applied[idx] = new RangeAggregatorFactory(org[idx], mean.lhs, mean.rhs);
            settlingApplied = true;
          }
        } else if (mean != null) {
          applied[idx] = new RangeAggregatorFactory(org[idx], mean.lhs, mean.rhs);
          settlingApplied = true;
        } else {
          applied[idx] = org[idx];
        }
      }
    } else {
      for (int idx = 0; idx < org.length; idx++)
      {
        applied[idx] = org[idx];
      }
    }

    return settlingApplied;
  }

  private String getAggCode(AggregatorFactory aggregatorFactory)
  {
    String className = aggregatorFactory.getClass().getSimpleName();

    switch(className)
    {
      case "DoubleMinAggregatorFactory":
      case "LongMinAggregatorFactory":
        return "MI";
      case "DoubleMaxAggregatorFactory":
      case "LongMaxAggregatorFactory":
        return "MA";
      case "ApproximateHistogramFoldingAggregatorFactory":
        return "MD";
      case "MetricRangeAggregatorFactory":
        return "RA";
      case "MetricAreaAggregatorFactory":
        return "AR";
      case "VarianceAggregatorFactory":
        return "ST";
    }

    return null;
  }

  private Map<String, Pair<Integer, Integer>> getValueMap(InputRow row)
  {
    int index = 0;
    for (String column: constColumns) {
      // it assumes that dimension value is not array
      List<String> constDims = row.getDimension(column);
      dimValues[index++] = (constDims.size() == 0) ? null : constDims.get(0);
    }
    MultiKey key = new MultiKey(dimValues, false);
    Map<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> matcherMap = settlingMap.get(key);

    if (matcherMap != null)
    {
      index = 0;
      for (String column: regexColumns) {
        // it assumes that dimension value is not array
        List<String> regexDims = row.getDimension(column);
        regexValues[index++] = (regexDims.size() == 0) ? null: regexDims.get(0);
      }

      for (Map.Entry<HadoopSettlingMatcher, Map<String, Pair<Integer, Integer>>> matcher: matcherMap.entrySet())
      {
        if (matcher.getKey().matches(regexValues)) {
          return matcher.getValue();
        }
      }
    }

    return null;
  }
}
