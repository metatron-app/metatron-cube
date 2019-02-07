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
import com.google.common.base.Preconditions;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;
import com.metamx.common.parsers.Parsers;
import io.druid.data.input.TimestampSpec;

import java.util.List;

/**
 */
public class CSVParseSpec extends AbstractParseSpec
{
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean trim;

  @JsonCreator
  public CSVParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("trim") boolean trim
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.listDelimiter = listDelimiter;
    Preconditions.checkNotNull(columns, "columns");
    for (String column : columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }
    ParserUtils.validateFields(columns);
    this.columns = columns;
    this.trim = trim;
  }

  public CSVParseSpec(
      DefaultTimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      String listDelimiter,
      List<String> columns
  )
  {
    this(timestampSpec, dimensionsSpec, listDelimiter, columns, false);
  }

  @JsonProperty
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public boolean isTrim()
  {
    return trim;
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    return new CSVParser(listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter, columns, trim);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CSVParseSpec(spec, getDimensionsSpec(), listDelimiter, columns, trim);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CSVParseSpec(getTimestampSpec(), spec, listDelimiter, columns, trim);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new CSVParseSpec(getTimestampSpec(), getDimensionsSpec(), listDelimiter, cols, trim);
  }
}
