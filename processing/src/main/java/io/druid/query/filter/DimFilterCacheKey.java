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

package io.druid.query.filter;

/**
 */
public interface DimFilterCacheKey
{
  byte NOOP_CACHE_ID = -0x4;
  byte AND_CACHE_ID = 0x1;
  byte OR_CACHE_ID = 0x2;
  byte NOT_CACHE_ID = 0x3;
  byte SELECTOR_CACHE_ID = 0x0;
  byte EXTRACTION_CACHE_ID = 0x4;
  byte REGEX_CACHE_ID = 0x5;
  byte SEARCH_QUERY_TYPE_ID = 0x6;
  byte JAVASCRIPT_CACHE_ID = 0x7;
  byte SPATIAL_CACHE_ID = 0x8;
  byte IN_CACHE_ID = 0x9;
  byte BOUND_CACHE_ID = 0xA;
  byte MATH_EXPR_CACHE_ID = 0xB;

  byte INS_CACHE_ID = 0x14;
  byte LIKE_CACHE_ID = 0x11;
  byte BLOOM_CACHE_ID = 0x12;
  byte PREFIX_CACHE_ID = 0x16;
  byte IS_NULL_CACHE_ID = 0x18;

  byte LUCENE_QUERY_CACHE_ID = 0xC;
  byte LUCENE_POINT_CACHE_ID = 0xD;
  byte LUCENE_NEAREST_CACHE_ID = 0xE;
  byte LUCENE_GEOJSON_CACHE_ID = 0xF;
  byte LUCENE_SPATIAL_CACHE_ID = 0x10;
  byte LUCENE_WITHIN_CACHE_ID = 0x13;
  byte LUCENE_KNN_CACHE_ID = 0x14;

  byte BOUND_ROWID_CACHE_ID = 0x15;
  byte H3_DISTANCE_CACHE_ID = 0x17;

  byte OTHER_PREFIX = 0x7f;
}
