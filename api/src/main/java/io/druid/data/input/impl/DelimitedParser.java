/*
 * Copyright 2011,2012 Metamarkets Group Inc.
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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;
import com.metamx.common.parsers.Parsers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// copied from com.metamx.common.parsers.DelimitedParser
public class DelimitedParser implements Parser<String, Object>
{
  private static final String DEFAULT_DELIMITER = "\t";

  private final String delimiter;
  private final String listDelimiter;

  private final Splitter splitter;
  private final Splitter listSplitter;

  private final List<String> columns;
  private final Set<String> listColumns;

  public DelimitedParser(
      Optional<String> delimiter,
      Optional<String> listDelimiter,
      List<String> columnNames,
      List<String> listColumnNames
  )
  {
    this.delimiter = delimiter.isPresent() ? delimiter.get() : DEFAULT_DELIMITER;
    this.listDelimiter = listDelimiter.isPresent() ? listDelimiter.get() : Parsers.DEFAULT_LIST_DELIMITER;

    Preconditions.checkState(
        !this.delimiter.equals(this.listDelimiter),
        "Cannot have same delimiter and list delimiter of [%s]",
        this.delimiter
    );

    Preconditions.checkState(columnNames != null && !columnNames.isEmpty(), "column name should not be empty");

    this.columns = columnNames;
    this.listColumns = listColumnNames == null ? null : Sets.newHashSet(listColumnNames);
    this.splitter = Splitter.on(this.delimiter);
    this.listSplitter = Splitter.on(this.listDelimiter);
  }

  public String getDelimiter()
  {
    return delimiter;
  }

  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @Override
  public List<String> getFieldNames()
  {
    return columns;
  }

  @Override
  public void setFieldNames(final Iterable<String> fieldNames)
  {
    throw new UnsupportedOperationException("getFieldNames");
  }

  public void setFieldNames(String header)
  {
    try {
      setFieldNames(splitter.split(header));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse header [%s]", header);
    }
  }

  @Override
  public Map<String, Object> parse(final String input)
  {
    Map<String, Object> row = Maps.newLinkedHashMap();
    try {
      Iterator<String> fields = splitter.split(input).iterator();
      for (int i = 0; i < columns.size(); i++) {
        String key = columns.get(i);
        if (!fields.hasNext()) {
          row.put(key, "");
        } else {
          String value = fields.next();
          if ((listColumns == null || listColumns.contains(key)) && value.contains(listDelimiter)) {
            row.put(
                key, Lists.newArrayList(
                    Iterables.transform(
                        listSplitter.split(value), ParserUtils.nullEmptyStringFunction
                    )
                )
            );
          } else {
            row.put(key, value);
          }
        }
      }
      return row;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }
}
