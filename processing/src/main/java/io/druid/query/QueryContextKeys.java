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
  public static final String FINAL_MERGE = "finalMerge";
  public static final String BY_SEGMENT = "bySegment";
  public static final String LOCAL_POST_PROCESSING = "localPostProcessing";
  public static final String DATETIME_CUSTOM_SERDE = "dateTimeCustomSerde"; // datetime serde between broker & others
  public static final String LOCAL_SPLIT_STRATEGY = "localSplitStrategy";
  public static final String USE_BULK_ROW = "useBulkRow";

  // group-by config.. overriding
  public static final String GBY_MERGE_PARALLELISM = "groupByMergeParallelism";
  public static final String GBY_CONVERT_TIMESERIES = "groupByConvertTimeseries";
  public static final String GBY_ESTIMATE_TOPN_FACTOR = "groupByEstimateTopNFactor";
  public static final String GBY_PRE_ORDERING = "groupByPreOrdering";
  public static final String GBY_REMOVE_ORDERING = "groupByRemoveOrdering";
  public static final String GBY_LOCAL_SPLIT_NUM = "groupByLocalSplitNum";
  public static final String GBY_LOCAL_SPLIT_CARDINALITY = "groupByLocalSplitCardinality";
  public static final String GBY_USE_RAW_UTF8 = "groupByUseRawUTF8";
  public static final String GBY_USE_BULK_ROW = "groupByUseBulkRow";
  public static final String GBY_MAX_STREAM_SUBQUERY_PAGE = "groupByMaxStreamSubQueryPage";

  public static final String STREAM_RAW_LOCAL_SPLIT_NUM = "streamRawLocalSplitNum";

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
      GBY_MERGE_PARALLELISM,
      GBY_CONVERT_TIMESERIES,
      GBY_ESTIMATE_TOPN_FACTOR,
      GBY_PRE_ORDERING,
      GBY_REMOVE_ORDERING,
      GBY_LOCAL_SPLIT_NUM,
      GBY_LOCAL_SPLIT_CARDINALITY,
      GBY_USE_RAW_UTF8,
      GBY_MAX_STREAM_SUBQUERY_PAGE,
      STREAM_RAW_LOCAL_SPLIT_NUM,
      USE_CACHE,
      POPULATE_CACHE,
      OPTIMIZE_QUERY,
      ALL_DIMENSIONS_FOR_EMPTY,
      ALL_METRICS_FOR_EMPTY
  );
}
