/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.ISE;
import com.metamx.common.parsers.JavaScriptParser;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.TimestampSpec;
import io.druid.js.JavaScriptConfig;

/**
 */
public class JavaScriptParseSpec extends AbstractParseSpec
{
  private final String function;
  private final JavaScriptConfig config;

  @JsonCreator
  public JavaScriptParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("function") String function,
      @JacksonInject JavaScriptConfig config
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.function = function;
    this.config = config;
  }

  @JsonProperty("function")
  public String getFunction()
  {
    return function;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    if (config.isDisabled()) {
      throw new ISE("JavaScript is disabled");
    }

    return new JavaScriptParser(function);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new JavaScriptParseSpec(spec, getDimensionsSpec(), function, config);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new JavaScriptParseSpec(getTimestampSpec(), spec, function, config);
  }
}
