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
import io.druid.data.input.InputRow;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.range.RangeAggregatorFactory;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class HadoopSettlingConfig implements SettlingConfig
{
  final private List<String> constColumns;
  final private List<String> regexColumns;
  final private String paramNameColumn;
  final private String paramValueColumn;
  final private String aggTypeColumn;
  final private String offsetColumn;
  final private String sizeColumn;
  final private String settlingYN;

  protected String DEFAULT_SETTLING_YN = "settling";

  public HadoopSettlingConfig(
      final List<String> constColumns,
      final List<String> regexColumns,
      final String paramNameColumn,
      final String paramValueColumn,
      final String aggTypeColumn,
      final String offset,
      final String size,
      final String settlingYN
  )
  {
    this.constColumns = Preconditions.checkNotNull(constColumns);
    this.regexColumns = Preconditions.checkNotNull(regexColumns);
    this.paramNameColumn = Preconditions.checkNotNull(paramNameColumn);
    this.paramValueColumn = Preconditions.checkNotNull(paramValueColumn);
    this.aggTypeColumn = Preconditions.checkNotNull(aggTypeColumn);
    this.offsetColumn = Preconditions.checkNotNull(offset);
    this.sizeColumn = Preconditions.checkNotNull(size);
    this.settlingYN = settlingYN == null ? DEFAULT_SETTLING_YN : settlingYN;
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

  @Override
  @JsonProperty("paramNameColumn")
  public String getParamNameColumn()
  {
    return paramNameColumn;
  }

  @Override
  @JsonProperty("paramValueColumn")
  public String getParamValueColumn()
  {
    return paramValueColumn;
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

  public Settler setUp(AggregatorFactory[] org, List<Map<String, Object>> resultMap)
  {
    return new HadoopSettler(this, org, resultMap);
  }

  private static class ObjectArray
  {
    private final Object[] array;

    private ObjectArray(Object[] array)
    {
      this.array = array;
    }

    @Override
    public boolean equals(Object o)
    {
      return Arrays.equals(array, ((ObjectArray) o).array);
    }

    @Override
    public int hashCode()
    {
      return Arrays.hashCode(array);
    }

    @Override
    public String toString()
    {
      return Arrays.toString(array);
    }
  }

  private static class HadoopSettler implements Settler
  {
    private final List<String> constColumns;
    private final List<String> regexColumns;
    private final String paramNameColumn;
    private final Map<ObjectArray, Map<HadoopSettlingMatcher, int[][]>> settlingMap;
    private final Object[] dimValues;
    private final String[] regexValues;

    private final int paramIndex;
    private final AggregatorFactory[] org;
    private final int[] codes;

    public HadoopSettler(HadoopSettlingConfig settlingConfig, AggregatorFactory[] org, List<Map<String, Object>> maps)
    {
      this.constColumns = settlingConfig.constColumns;
      this.regexColumns = settlingConfig.regexColumns;
      this.paramNameColumn = settlingConfig.paramNameColumn;
      this.settlingMap = Maps.newHashMap();

      this.org = org;
      this.codes = CODE.getAggCode(org);
      this.paramIndex = constColumns.indexOf(paramNameColumn);
      if (paramIndex < 0) {
        throw new IllegalArgumentException("paramNameColumn should be included in constColumns");
      }

      dimValues = new Object[constColumns.size()];
      regexValues = new String[regexColumns.size()];

      HadoopSettlingMatcherFactory matcherFactory = new HadoopSettlingMatcherFactory();

      for (Map<String, Object> row : maps) {
        Object[] keys = new Object[constColumns.size()];
        int idx = 0;
        for (String column : constColumns) {
          keys[idx++] = row.get(column);
        }
        ObjectArray key = new ObjectArray(keys);
        Map<HadoopSettlingMatcher, int[][]> settlingMatcherMap = settlingMap.get(key);
        if (settlingMatcherMap == null) {
          settlingMatcherMap = Maps.newHashMap();
          settlingMap.put(key, settlingMatcherMap);
        }

        String[] regexKeys = new String[regexColumns.size()];
        idx = 0;
        for (String column : regexColumns) {
          regexKeys[idx++] = (String) row.get(column);
        }
        HadoopSettlingMatcher settlingMatcher = matcherFactory.getSettlingMatcher(regexKeys);
        int[][] settlings = settlingMatcherMap.get(settlingMatcher);
        if (settlings == null) {
          settlingMatcherMap.put(settlingMatcher, settlings = new int[CODE.values().length][2]);
        }
        String typeName = (String) row.get(settlingConfig.aggTypeColumn);
        CODE type = CODE.fromString(typeName);
        if (type != null) {
          String settling = (String) row.get(settlingConfig.offsetColumn);
          String activation = (String) row.get(settlingConfig.sizeColumn);
          settlings[type.ordinal()][0] = (int) Float.parseFloat(settling);
          settlings[type.ordinal()][1] = (int) Float.parseFloat(activation);
        }
      }

      matcherFactory.clear();
    }

    @Override
    public AggregatorFactory[][] applySettling(InputRow row)
    {
      int[][][] settlings = getValueMap(row);

      if (settlings != null) {
        AggregatorFactory[][] applied = new AggregatorFactory[settlings.length][];
        for (int i = 0; i < applied.length; i++) {
          applied[i] = new AggregatorFactory[codes.length];
          if (settlings[i] == null) {
            System.arraycopy(org, 0, applied[i], 0, org.length);
            continue;
          }
          int[][] settling = settlings[i];  // settling for param
          for (int idx = 0; idx < codes.length; idx++) {
            int code = codes[idx];
            if (settling[code] != null) {
              applied[i][idx] = new RangeAggregatorFactory(org[idx], settling[code][0], settling[code][1]);
            } else {
              applied[i][idx] = org[idx];
            }
          }
        }
        return applied;
      }

      return null;
    }

    private int[][][] getValueMap(InputRow row)
    {
      List<String> dimensions = row.getDimension(paramNameColumn);
      int index = 0;
      for (int i = 0; i < constColumns.size(); i++) {
        // it assumes that dimension value is not array
        if (index != paramIndex) {
          dimValues[index] = row.getRaw(constColumns.get(i));
        }
        index++;
      }
      for (int i = 0; i < regexColumns.size(); i++) {
        // it assumes that dimension value is not array
        Object regexDims = row.getRaw(regexColumns.get(i));
        regexValues[i] = regexDims == null ? null : String.valueOf(regexDims);
      }

      int[][][] result = new int[dimensions.size()][][];
      for (int i = 0; i < result.length; i++) {
        dimValues[paramIndex] = dimensions.get(i);
        Map<HadoopSettlingMatcher, int[][]> matcherMap = settlingMap.get(new ObjectArray(dimValues));
        if (matcherMap != null) {
          for (Map.Entry<HadoopSettlingMatcher, int[][]> matcher : matcherMap.entrySet()) {
            if (matcher.getKey().matches(regexValues)) {
              result[i] = matcher.getValue();
              break;
            }
          }
        }
      }
      return result;
    }
  }

  // ignore NULL CO IN MF S1 SL
  private static enum CODE
  {
    ME, MI, MA, MD, RA, AR, ST;

    private static CODE fromString(String name)
    {
      try {
        if (name != null) {
          return CODE.valueOf(name.toUpperCase());
        }
      }
      catch (IllegalArgumentException e) {
      }
      return null;
    }

    private static int[] getAggCode(AggregatorFactory[] aggregatorFactory)
    {
      int[] codes = new int[aggregatorFactory.length];
      for (int i = 0; i < codes.length; i++) {
        String className = aggregatorFactory[i].getClass().getSimpleName();

        switch (className) {
          case "DoubleMinAggregatorFactory":
          case "LongMinAggregatorFactory":
            codes[i] = MI.ordinal();
            continue;
          case "DoubleMaxAggregatorFactory":
          case "LongMaxAggregatorFactory":
            codes[i] = MA.ordinal();
            continue;
          case "ApproximateHistogramAggregatorFactory":
          case "ApproximateHistogramFoldingAggregatorFactory":
          case "DruidTDigestAggregatorFactory":
            codes[i] = MD.ordinal();
            continue;
          case "MetricRangeAggregatorFactory":
            codes[i] = RA.ordinal();
            continue;
          case "MetricAreaAggregatorFactory":
            codes[i] = AR.ordinal();
            continue;
          case "VarianceAggregatorFactory":
            codes[i] = ST.ordinal();
            continue;
          default:
            codes[i] = ME.ordinal();
        }
      }
      return codes;
    }
  }
}
