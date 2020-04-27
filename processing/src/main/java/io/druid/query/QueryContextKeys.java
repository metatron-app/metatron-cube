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

package io.druid.query;

import java.util.Arrays;
import java.util.List;

public interface QueryContextKeys
{
  String QUERYID = "queryId";

  // internal markers
  String PRIORITY = "priority";
  String TIMEOUT = "timeout";
  String FINALIZE = "finalize";
  String FINAL_MERGE = "finalMerge";
  String BY_SEGMENT = "bySegment";
  String LOCAL_POST_PROCESSING = "#localPostProcessing";
  String DATETIME_CUSTOM_SERDE = "#dateTimeCustomSerde"; // datetime serde between broker & others
  String LOCAL_SPLIT_STRATEGY = "#localSplitStrategy";
  String FUDGE_TIMESTAMP = "#fudgeTimestamp";
  String USE_BULK_ROW = "#useBulkRow";
  String MAJOR_TYPES = "#majorTypes";   // for sketch
  String DISABLE_LOG = "#disableLog";

  String USE_CUBOIDS = "useCuboids";

  // group-by config.. overriding
  String GBY_USE_PARALLEL_SORT = "groupByUseParallelSort";
  String GBY_CONVERT_TIMESERIES = "groupByConvertTimeseries";
  String GBY_CONVERT_FREQUENCY = "groupByConvertFrequency";
  String GBY_ESTIMATE_TOPN_FACTOR = "groupByEstimateTopNFactor";
  String GBY_PRE_ORDERING = "groupByPreOrdering";
  String GBY_REMOVE_ORDERING = "groupByRemoveOrdering";
  String GBY_LOCAL_SPLIT_NUM = "groupByLocalSplitNum";
  String GBY_LOCAL_SPLIT_CARDINALITY = "groupByLocalSplitCardinality";
  String GBY_USE_RAW_UTF8 = "groupByUseRawUTF8";
  String GBY_USE_BULK_ROW = "groupByUseBulkRow";
  String GBY_MAX_STREAM_SUBQUERY_PAGE = "groupByMaxStreamSubQueryPage";

  // select.stream.raw
  String STREAM_USE_BULK_ROW = "streamUseBulkRow";
  String STREAM_USE_RAW_UTF8 = "streamUseRawUTF8";
  String STREAM_RAW_LOCAL_SPLIT_NUM = "streamRawLocalSplitNum";
  String STREAM_RAW_LOCAL_SPLIT_ROWS = "streamRawLocalSplitRows";

  // frequency
  String FREQUENCY_SKETCH_DEPTH = "frequencySketchDepth";

  // join
  String HASHJOIN_THRESHOLD = "hashjoinThreshold";

  // CacheConfig
  String USE_CACHE = "useCache";
  String POPULATE_CACHE = "populateCache";

  String OPTIMIZE_QUERY = "optimizeQuery";
  String POST_PROCESSING = "postProcessing";
  String ALL_DIMENSIONS_FOR_EMPTY = "allDimensionsForEmpty";
  String ALL_METRICS_FOR_EMPTY = "allMetricsForEmpty";
  String FORWARD_URL = "forwardURL";
  String FORWARD_CONTEXT = "forwardContext";
  String DATETIME_STRING_SERDE = "dateTimeStringSerde";   // use string always

  // for jmx
  String PREVIOUS_JMX = "previousJmx";

  // forward context
  String FORWARD_TIMESTAMP_COLUMN = "timestampColumn";
  String FORWARD_PARALLEL = "parallel";

  // generic
  String MAX_RESULTS = "maxResults";
  String DECORATOR_CONTEXT = "decoratorContext";
  String MAX_QUERY_PARALLELISM = "maxQueryParallelism";

  List<String> FOR_META = Arrays.asList(
      QUERYID,
      PRIORITY,
      TIMEOUT,
      USE_CUBOIDS,
      GBY_USE_PARALLEL_SORT,
      GBY_CONVERT_TIMESERIES,
      GBY_CONVERT_FREQUENCY,
      GBY_ESTIMATE_TOPN_FACTOR,
      GBY_PRE_ORDERING,
      GBY_REMOVE_ORDERING,
      GBY_LOCAL_SPLIT_NUM,
      GBY_LOCAL_SPLIT_CARDINALITY,
      GBY_USE_RAW_UTF8,
      GBY_USE_BULK_ROW,
      GBY_MAX_STREAM_SUBQUERY_PAGE,
      STREAM_USE_RAW_UTF8,
      STREAM_USE_BULK_ROW,
      STREAM_RAW_LOCAL_SPLIT_NUM,
      STREAM_RAW_LOCAL_SPLIT_ROWS,
      FREQUENCY_SKETCH_DEPTH,
      HASHJOIN_THRESHOLD,
      USE_CACHE,
      POPULATE_CACHE,
      OPTIMIZE_QUERY,
      ALL_DIMENSIONS_FOR_EMPTY,
      ALL_METRICS_FOR_EMPTY,
      MAX_RESULTS,
      MAX_QUERY_PARALLELISM
  );
}
