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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.ValueDesc;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;
import io.druid.java.util.common.collect.Utils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StringInputRowParser.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "string", value = StringInputRowParser.class),
    @JsonSubTypes.Type(name = "map", value = MapInputRowParser.class),
    @JsonSubTypes.Type(name = "noop", value = NoopInputRowParser.class),
    @JsonSubTypes.Type(name = "csv.stream", value = CSVInputRowParser.class),
})
// not for serialization
public interface InputRowParser<T>
{
  /**
   * Parse an input into list of {@link InputRow}. List can contains null for rows that should be thrown away,
   * or throws {@code ParseException} if the input is unparseable. This method should never return null otherwise
   * lots of things will break.
   */
  @NotNull
  default List<InputRow> parseBatch(T input)
  {
    return Utils.nullableListOf(parse(input));
  }

  /**
   * Parse an input into an {@link InputRow}. Return null if this input should be thrown away, or throws
   * {@code ParseException} if the input is unparseable.
   */
  @Nullable
  default InputRow parse(T input)
  {
    return null;
  }

  // returns known types before parsing, if possible
  default Map<String, ValueDesc> knownTypes()
  {
    return null;
  }

  @JsonIgnore
  TimestampSpec getTimestampSpec();

  @JsonIgnore
  DimensionsSpec getDimensionsSpec();

  InputRowParser withDimensionExclusions(Set<String> exclusions);

  interface Streaming<T> extends InputRowParser<T>
  {
    boolean accept(Object input);

    Iterator<InputRow> parseStream(Object input) throws IOException;

    // not like other parsers handling 'ignoreInvalidRows' in outside, it shoud be done in inside
    // exceptions thrown in hasNext() will always be propagated cause it's not possible to overcome that in streaming world
    Streaming<T> withIgnoreInvalidRows(boolean ignoreInvalidRows);
  }

  interface Delegated<T> extends Streaming<T>
  {
    InputRowParser<T> getDelegate();
  }
}
