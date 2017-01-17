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

public interface QueryContextKeys
{
  // internal
  public static final String PRIORITY = "priority";
  public static final String TIMEOUT = "timeout";
  public static final String CHUNK_PERIOD = "chunkPeriod";
  public static final String FINALIZE = "finalize";
  public static final String BY_SEGMENT = "bySegment";

  // CacheConfig
  public static final String USE_CACHE = "useCache";
  public static final String POPULATE_CACHE = "populateCache";

  public static final String OPTIMIZE_QUERY = "optimizeQuery";
  public static final String POST_PROCESSING = "postProcessing";
  public static final String ALL_COLUMNS_FOR_EMPTY = "allColumnsForEmpty";
  public static final String FORWARD_URL = "forwardURL";
  public static final String FORWARD_CONTEXT = "forwardContext";

  // forward context
  public static final String FORWARD_TIMESTAMP_COLUMN = "timestampColumn";
  public static final String FORWARD_PARALLEL = "parallel";
  public static final String FORWARD_PREFIX_LOCATION = "wrapWithLocation";
}
