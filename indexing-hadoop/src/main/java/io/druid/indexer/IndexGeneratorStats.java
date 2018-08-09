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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.mapreduce.Counters;

import static io.druid.indexer.HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER;

/**
 */
public class IndexGeneratorStats
{
  private long invalidRowCount = 0;
  private long indexedRows = 0;
  private long oobRows = 0;
  private long errRows = 0;
  private long flushedIndex = 0;

  public void setStats(Counters counters)
  {
    this.invalidRowCount = counters.findCounter(INVALID_ROW_COUNTER).getValue();
    this.indexedRows = counters.findCounter("druid.internal", "indexed-row-num").getValue();
    this.oobRows = counters.findCounter("druid.internal", "oob-row-num").getValue();
    this.errRows = counters.findCounter("druid.internal", "err-row-num").getValue();
    this.flushedIndex = counters.findCounter("druid.internal", "index-flush-count").getValue();
  }

  @JsonProperty
  public long getInvalidRowCount()
  {
    return invalidRowCount;
  }

  @JsonProperty
  public long getIndexedRows()
  {
    return indexedRows;
  }

  @JsonProperty
  public long getOobRows()
  {
    return oobRows;
  }

  @JsonProperty
  public long getErrRows()
  {
    return errRows;
  }

  @JsonProperty
  public long getFlushedIndex()
  {
    return flushedIndex;
  }
}