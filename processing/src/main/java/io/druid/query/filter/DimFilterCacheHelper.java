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
public class DimFilterCacheHelper
{
  static final byte NOOP_CACHE_ID = -0x4;
  static final byte SELECTOR_CACHE_ID = 0x0;
  static final byte AND_CACHE_ID = 0x1;
  static final byte OR_CACHE_ID = 0x2;
  static final byte NOT_CACHE_ID = 0x3;
  static final byte EXTRACTION_CACHE_ID = 0x4;
  static final byte REGEX_CACHE_ID = 0x5;
  static final byte SEARCH_QUERY_TYPE_ID = 0x6;
  static final byte JAVASCRIPT_CACHE_ID = 0x7;
  static final byte SPATIAL_CACHE_ID = 0x8;
  static final byte IN_CACHE_ID = 0x9;
  static final byte BOUND_CACHE_ID = 0xA;
  static final byte MATH_EXPR_CACHE_ID = 0xB;
  static final byte LUCENE_QUERY_CACHE_ID = 0xC;
  static final byte LUCENE_POINT_CACHE_ID = 0xD;
  static final byte LUCENE_NEAREST_CACHE_ID = 0xE;
  static final byte LUCENE_GEOJSON_CACHE_ID = 0xF;
  static final byte LUCENE_SPATIAL_CACHE_ID = 0x10;
  static final byte LIKE_CACHE_ID = 0x11;
}
