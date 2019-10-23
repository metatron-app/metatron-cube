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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.parsers.Parser;
import io.druid.data.input.TimestampSpec;

import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "format", defaultImpl = DelimitedParseSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "json", value = JSONParseSpec.class),
    @JsonSubTypes.Type(name = "csv", value = CSVParseSpec.class),
    @JsonSubTypes.Type(name = "tsv", value = DelimitedParseSpec.class),
    @JsonSubTypes.Type(name = "jsonLowercase", value = JSONLowercaseParseSpec.class),
    @JsonSubTypes.Type(name = "timeAndDims", value = TimeAndDimsParseSpec.class),
    @JsonSubTypes.Type(name = "regex", value = RegexParseSpec.class),
    @JsonSubTypes.Type(name = "javascript", value = JavaScriptParseSpec.class),
    @JsonSubTypes.Type(name = "nested", value = NestedParseSpec.class)
})
public interface ParseSpec
{
  TimestampSpec getTimestampSpec();

  DimensionsSpec getDimensionsSpec();

  Parser<String, Object> makeParser();

  ParseSpec withTimestampSpec(TimestampSpec spec);

  ParseSpec withDimensionsSpec(DimensionsSpec spec);

  abstract class AbstractParser<K, V> implements io.druid.java.util.common.parsers.Parser<K, V>
  {
    @Override
    public final void setFieldNames(Iterable<String> fieldNames)
    {
      throw new UnsupportedOperationException("deprecated");
    }

    @Override
    public final List<String> getFieldNames()
    {
      throw new UnsupportedOperationException("deprecated");
    }
  }
}
