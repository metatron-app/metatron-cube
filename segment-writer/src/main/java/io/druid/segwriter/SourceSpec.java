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

package io.druid.segwriter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * Where the rows to ingest come from. Polymorphic on {@code type} so new sources (kafka, jdbc, …) can
 * be added without touching the rest of the spec. The actual Spark read lives in the spark-ingestion
 * module (which has the Spark dependency); this type only carries the declarative config.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "file", value = FileSource.class),
    @JsonSubTypes.Type(name = "iceberg", value = IcebergSource.class)
})
public abstract class SourceSpec implements Serializable
{
  private final String where;

  protected SourceSpec(String where)
  {
    this.where = where;
  }

  /** optional SQL predicate applied to the source (e.g. partition pruning for an iceberg table). */
  @JsonProperty
  public String getWhere()
  {
    return where;
  }
}
