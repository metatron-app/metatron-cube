/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;

import java.util.Iterator;
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
  InputRow parse(T input);

  @JsonIgnore
  TimestampSpec getTimestampSpec();

  @JsonIgnore
  DimensionsSpec getDimensionsSpec();

  InputRowParser withDimensionExclusions(Set<String> exclusions);

  interface Streaming<T> extends InputRowParser<T>
  {
    boolean accept(Object input);

    Iterator<InputRow> parseStream(Object input);
  }

  interface Delegated<T> extends Streaming<T>
  {
    InputRowParser<T> getDelegate();
  }
}
