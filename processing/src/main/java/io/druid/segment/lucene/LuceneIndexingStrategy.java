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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import io.druid.data.ValueDesc;
import org.apache.lucene.document.Field;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "text", value = TextIndexingStrategy.class),
    @JsonSubTypes.Type(name = "latlon", value = LatLonPointIndexingStrategy.class)
})
public interface LuceneIndexingStrategy
{
  String TEXT_DESC = "text";
  String LATLON_POINT_DESC = "point";

  String getFieldName();

  String getFieldDescriptor();

  LuceneIndexingStrategy withFieldName(String fieldName);

  Function<Object, Field[]> createIndexableField(ValueDesc type);
}
