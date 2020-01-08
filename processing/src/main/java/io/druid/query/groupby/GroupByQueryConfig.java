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

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 */
public class GroupByQueryConfig
{
  @JsonProperty
  private boolean singleThreaded = false;

  @JsonProperty
  @Min(1000)
  private int maxResults = 500000;

  @JsonProperty
  private boolean sortOnTime = true;

  @JsonProperty
  @Min(1)
  private int maxMergeParallelism = 8;

  @JsonProperty
  private boolean preOrdering = false;

  @JsonProperty
  private boolean removeOrdering = false;

  @JsonProperty
  private boolean convertTimeseries = false;

  @JsonProperty
  private boolean convertFrequency = false;

  @JsonProperty
  private int convertFrequencyCardinality = 1 << 18;

  @JsonProperty
  private int estimateTopNFactor = -1;

  @JsonProperty
  private boolean useParallelSort = true;

  @JsonProperty
  private boolean useRawUTF8 = false;

  @JsonProperty
  private boolean useBulkRow = false;

  @JsonProperty
  private int localSplitNum = -1;

  @JsonProperty
  private int localSplitCardinality = -1;

  @JsonProperty
  private int maxStreamSubQueryPage = 4;

  public boolean isSingleThreaded()
  {
    return singleThreaded;
  }

  public void setSingleThreaded(boolean singleThreaded)
  {
    this.singleThreaded = singleThreaded;
  }

  public int getMaxResults()
  {
    return maxResults;
  }

  public void setMaxResults(int maxResults)
  {
    this.maxResults = maxResults;
  }

  public boolean isSortOnTime()
  {
    return sortOnTime;
  }

  public void setSortOnTime(boolean sortOnTime)
  {
    this.sortOnTime = sortOnTime;
  }

  public int getMaxMergeParallelism()
  {
    return maxMergeParallelism;
  }

  public void setMaxMergeParallelism(int maxMergeParallelism)
  {
    this.maxMergeParallelism = maxMergeParallelism;
  }

  public boolean isPreOrdering()
  {
    return preOrdering;
  }

  public void setPreOrdering(boolean preOrdering)
  {
    this.preOrdering = preOrdering;
  }

  public boolean isRemoveOrdering()
  {
    return removeOrdering;
  }

  public void setRemoveOrdering(boolean removeOrdering)
  {
    this.removeOrdering = removeOrdering;
  }

  public boolean isConvertTimeseries()
  {
    return convertTimeseries;
  }

  public void setConvertFrequency(boolean convertFrequency)
  {
    this.convertFrequency = convertFrequency;
  }

  public boolean isConvertFrequency()
  {
    return convertFrequency;
  }

  public void setConvertTimeseries(boolean convertTimeseries)
  {
    this.convertTimeseries = convertTimeseries;
  }

  public int getConvertFrequencyCardinality()
  {
    return convertFrequencyCardinality;
  }

  public void setConvertFrequencyCardinality(int convertFrequencyCardinality)
  {
    this.convertFrequencyCardinality = convertFrequencyCardinality;
  }

  public int getEstimateTopNFactor()
  {
    return estimateTopNFactor;
  }

  public void setEstimateTopNFactor(int estimateTopNFactor)
  {
    this.estimateTopNFactor = estimateTopNFactor;
  }

  public boolean isUseParallelSort()
  {
    return useParallelSort;
  }

  public void setUseParallelSort(boolean useParallelSort)
  {
    this.useParallelSort = useParallelSort;
  }

  public boolean isUseRawUTF8()
  {
    return useRawUTF8;
  }

  public void setUseRawUTF8(boolean useRawUTF8)
  {
    this.useRawUTF8 = useRawUTF8;
  }

  public boolean isUseBulkRow()
  {
    return useBulkRow;
  }

  public void setUseBulkRow(boolean useBulkRow)
  {
    this.useBulkRow = useBulkRow;
  }

  public int getLocalSplitNum()
  {
    return localSplitNum;
  }

  public void setLocalSplitNum(int localSplitNum)
  {
    this.localSplitNum = localSplitNum;
  }

  public int getLocalSplitCardinality()
  {
    return localSplitCardinality;
  }

  public void setLocalSplitCardinality(int localSplitCardinality)
  {
    this.localSplitCardinality = localSplitCardinality;
  }

  public int getMaxStreamSubQueryPage()
  {
    return maxStreamSubQueryPage;
  }

  public void setMaxStreamSubQueryPage(int maxStreamSubQueryPage)
  {
    this.maxStreamSubQueryPage = maxStreamSubQueryPage;
  }
}
