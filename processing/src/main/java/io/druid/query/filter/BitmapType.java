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

import java.util.EnumSet;

/**
 */
public enum BitmapType
{
  DIMENSIONAL,
  LUCENE_INDEX,
  HISTOGRAM_BITMAP,
  BSB;

  public static EnumSet<BitmapType> EXACT = EnumSet.of(DIMENSIONAL, LUCENE_INDEX, BSB);

  public static EnumSet<BitmapType> HELPER = EnumSet.of(DIMENSIONAL, HISTOGRAM_BITMAP);

  public static EnumSet<BitmapType> ALL = EnumSet.allOf(BitmapType.class);
}
