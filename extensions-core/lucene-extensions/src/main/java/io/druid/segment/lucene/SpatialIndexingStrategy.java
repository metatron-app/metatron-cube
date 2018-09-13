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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 */
@Deprecated
@JsonTypeName("spatial")
public class SpatialIndexingStrategy extends ShapeIndexingStrategy
{
  @JsonCreator
  public SpatialIndexingStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("shapeFormat") ShapeFormat shapeFormat,
      @JsonProperty("shapeType") ShapeType shapeType,
      @JsonProperty("maxLevels") int maxLevels
  )
  {
    super(fieldName, shapeFormat, shapeType, maxLevels);
  }
}
