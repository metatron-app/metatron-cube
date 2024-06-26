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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;

import java.util.Set;

/**
 */
public class NoopInputRowParser implements InputRowParser<InputRow>
{
  private final ParseSpec parseSpec;

  @JsonCreator
  public NoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec != null ? parseSpec : new TimeAndDimsParseSpec(null, null);
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRow parse(InputRow input)
  {
    return input;
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return parseSpec.getTimestampSpec();
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return parseSpec.getDimensionsSpec();
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new NoopInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions))
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NoopInputRowParser that = (NoopInputRowParser) o;

    return parseSpec.equals(that.parseSpec);

  }

  @Override
  public int hashCode()
  {
    return parseSpec.hashCode();
  }
}
