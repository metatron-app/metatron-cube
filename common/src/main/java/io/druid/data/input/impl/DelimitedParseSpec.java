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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.TimestampSpec;
import io.druid.java.util.common.parsers.Parser;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends AbstractParseSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;
  private final List<String> listColumns;
  private final String nullString;
  private final boolean trim;
  private final boolean dequote;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("listColumns") List<String> listColumns,
      @JsonProperty("nullString") String nullString,
      @JsonProperty("dequote") boolean dequote,
      @JsonProperty("trim") boolean trim
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;
    this.listColumns = listColumns;
    Preconditions.checkNotNull(columns, "columns");
    this.columns = columns;
    for (String column : this.columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }
    this.nullString = nullString;
    this.trim = trim;
    this.dequote = dequote;
  }

  public DelimitedParseSpec(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      String delimiter,
      String listDelimiter,
      List<String> columns
  )
  {
    this(timestampSpec, dimensionsSpec, delimiter, listDelimiter, columns, null, null, false, false);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getListColumns()
  {
    return listColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getNullString()
  {
    return nullString;
  }

  @JsonProperty
  public boolean isDequote()
  {
    return dequote;
  }

  @JsonProperty
  public boolean isTrim()
  {
    return trim;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new DelimitedParser(
        delimiter,
        listDelimiter,
        columns,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(
        spec,
        getDimensionsSpec(),
        delimiter,
        listDelimiter,
        columns,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(
        getTimestampSpec(),
        spec,
        delimiter,
        listDelimiter,
        columns,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }

  public ParseSpec withDelimiter(String delim)
  {
    return new DelimitedParseSpec(
        getTimestampSpec(),
        getDimensionsSpec(),
        delim,
        listDelimiter,
        columns,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }

  public ParseSpec withListDelimiter(String delim)
  {
    return new DelimitedParseSpec(
        getTimestampSpec(),
        getDimensionsSpec(),
        delimiter,
        delim,
        columns,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new DelimitedParseSpec(
        getTimestampSpec(),
        getDimensionsSpec(),
        delimiter,
        listDelimiter,
        cols,
        listColumns,
        nullString,
        dequote,
        trim
    );
  }
}
