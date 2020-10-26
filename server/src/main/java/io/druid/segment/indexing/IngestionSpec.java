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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.data.input.impl.InputRowParser;

/**
 */
public abstract class IngestionSpec<IOConfigType extends IOConfig, TuningConfigType extends TuningConfig>
{
  protected final DataSchema dataSchema;
  protected final IOConfigType ioConfig;
  protected final TuningConfigType tuningConfig;

  @JsonCreator
  public IngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") IOConfigType ioConfig,
      @JsonProperty("tuningConfig") TuningConfigType tuningConfig
  )
  {
    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig;
  }

  @JsonProperty("dataSchema")
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  public IOConfigType getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  public TuningConfigType getTuningConfig()
  {
    return tuningConfig;
  }

  public InputRowParser getParser(ObjectMapper mapper)
  {
    return dataSchema.getParser(mapper, tuningConfig.isIgnoreInvalidRows());
  }

  public boolean isDimensionFixed()
  {
    return dataSchema.isDimensionFixed();
  }

  public abstract IngestionSpec<IOConfigType, TuningConfigType> withDataSchema(DataSchema dataSchema);
}
