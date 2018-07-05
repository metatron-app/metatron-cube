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

package io.druid.query;

import java.util.Arrays;
import java.util.List;

public interface QueryContextKeys
{
  public static final String QUERYID = "queryId";

  // internal
  public static final String PRIORITY = "priority";
  public static final String TIMEOUT = "timeout";
  public static final String CHUNK_PERIOD = "chunkPeriod";
  public static final String FINALIZE = "finalize";
  public static final String BY_SEGMENT = "bySegment";
  public static final String FINAL_WORK = "finalWork";
  public static final String LOCAL_POST_PROCESSING = "localPostProcessing";
  public static final String DATETIME_CUSTOM_SERDE = "dateTimeCustomSerde"; // datetime serde between broker & others

  // group-by config.. overriding
  public static final String GBY_MERGE_PARALLELISM = "groupByMergeParallelism";
  public static final String GBY_CONVERT_TIMESERIES = "groupByConvertTimeseries";
  public static final String GBY_MERGE_SIMPLE = "groupByMergeSimple";
  public static final String GBY_COMPACT_TRANSFER = "groupByCompactTransfer";
  public static final String GBY_PRE_ORDERING = "groupByPreOrdering";
  public static final String GBY_REMOVE_ORDERING = "groupByRemoveOrdering";
  public static final String GBY_LOCAL_SPLIT_NUM = "groupByLocalSplitNum";
  public static final String GBY_MAX_STREAM_SUBQUERY_PAGE = "groupByMaxStreamSubQueryPage";

  public static final String RAW_LOCAL_SPLIT_NUM = "rawLocalSplitNum";

  // CacheConfig
  public static final String USE_CACHE = "useCache";
  public static final String POPULATE_CACHE = "populateCache";

  public static final String OPTIMIZE_QUERY = "optimizeQuery";
  public static final String POST_PROCESSING = "postProcessing";
  public static final String ALL_DIMENSIONS_FOR_EMPTY = "allDimensionsForEmpty";
  public static final String ALL_METRICS_FOR_EMPTY = "allMetricsForEmpty";
  public static final String FORWARD_URL = "forwardURL";
  public static final String FORWARD_CONTEXT = "forwardContext";
  public static final String DATETIME_STRING_SERDE = "dateTimeStringSerde";   // use string always

  // for sketch
  public static final String MAJOR_TYPES = "majorTypes";

  // for jmx
  public static final String PREVIOUS_JMX = "previousJmx";

  // forward context
  public static final String FORWARD_TIMESTAMP_COLUMN = "timestampColumn";
  public static final String FORWARD_PARALLEL = "parallel";

  public static final List<String> FOR_META = Arrays.asList(
      QUERYID,
      PRIORITY,
      TIMEOUT,
      GBY_COMPACT_TRANSFER,
      GBY_CONVERT_TIMESERIES,
      GBY_LOCAL_SPLIT_NUM,
      GBY_MERGE_PARALLELISM,
      GBY_MERGE_SIMPLE,
      GBY_MERGE_SIMPLE,
      GBY_PRE_ORDERING,
      GBY_REMOVE_ORDERING,
      RAW_LOCAL_SPLIT_NUM,
      USE_CACHE,
      POPULATE_CACHE,
      ALL_DIMENSIONS_FOR_EMPTY,
      ALL_METRICS_FOR_EMPTY
  );
}
