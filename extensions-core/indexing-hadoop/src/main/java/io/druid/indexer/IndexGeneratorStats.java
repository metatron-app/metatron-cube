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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.mapreduce.Counters;

/**
 */
public class IndexGeneratorStats
{
  private long indexedRows = 0;
  private long oobRows = 0;
  private long errRows = 0;
  private long nullRows = 0;
  private long flushedIndex = 0;

  public void setStats(Counters counters)
  {
    this.indexedRows = counters.findCounter("druid.internal", "indexed-row-num").getValue();
    this.oobRows = counters.findCounter("druid.internal", "oob-row-num").getValue();
    this.errRows = counters.findCounter("druid.internal", "err-row-num").getValue();
    this.nullRows = counters.findCounter("druid.internal", "null-row-num").getValue();
    this.flushedIndex = counters.findCounter("druid.internal", "index-flush-count").getValue();
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
  public long getNullRows()
  {
    return nullRows;
  }

  @JsonProperty
  public long getFlushedIndex()
  {
    return flushedIndex;
  }
}