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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.TimestampSpec;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends ParseSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;
  private final List<String> listColumns;
  private final boolean dequote;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("listColumns") List<String> listColumns,
      @JsonProperty("dequote") boolean dequote
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
    this(timestampSpec, dimensionsSpec, delimiter, listDelimiter, columns, null, false);
  }

  @JsonProperty("delimiter")
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty("listDelimiter")
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty("columns")
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty("listColumns")
  public List<String> getListColumns()
  {
    return listColumns;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new DelimitedParser(
        Optional.fromNullable(delimiter),
        Optional.fromNullable(listDelimiter),
        columns,
        listColumns,
        dequote
    );
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(spec, getDimensionsSpec(), delimiter, listDelimiter, columns, listColumns, dequote);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(getTimestampSpec(), spec, delimiter, listDelimiter, columns, listColumns, dequote);
  }

  public ParseSpec withDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delim, listDelimiter, columns, listColumns, dequote);
  }

  public ParseSpec withListDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, delim, columns, listColumns, dequote);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, listDelimiter, cols, listColumns, dequote);
  }
}
