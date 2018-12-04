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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.Parsers;

import java.util.List;
import java.util.Map;
import java.util.Set;

// copied from com.metamx.common.parsers.DelimitedParser
public class DelimitedParser implements Parser<String, Object>
{
  private static final String DEFAULT_DELIMITER = "\t";

  private final String delimiter;
  private final String listDelimiter;

  private final List<String> columns;
  private final Set<String> listColumns;

  private final boolean dequote;

  public DelimitedParser(
      Optional<String> delimiter,
      Optional<String> listDelimiter,
      List<String> columnNames,
      List<String> listColumnNames,
      boolean dequote
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
    this.dequote = dequote;
  }

  public DelimitedParser(
      Optional<String> delimiter,
      Optional<String> listDelimiter,
      List<String> columnNames
  )
  {
    this(delimiter, listDelimiter, columnNames, null, false);
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
    throw new UnsupportedOperationException("setFieldNames");
  }

  @Override
  public Map<String, Object> parse(final String input)
  {
    Map<String, Object> row = Maps.newLinkedHashMap();
    try {
      List<String> fields = split(input, delimiter);
      int limit = Math.min(columns.size(), fields.size());
      for (int i = 0; i < limit; i++) {
        String key = columns.get(i);
        String value = fields.get(i);
        if (Strings.isNullOrEmpty(value)) {
          row.put(key, null);
          continue;
        }
        if (dequote && value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
          value = value.substring(1, value.length() - 1);
        }
        if ((listColumns == null || listColumns.contains(key)) && value.contains(listDelimiter)) {
          List<String> elements = Lists.newArrayList();
          for (String element : split(value, listDelimiter)) {
            elements.add(Strings.isNullOrEmpty(element) ? null : element);
          }
          row.put(key, elements);
        } else {
          row.put(key, value);
        }
      }
      return row;
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  public static List<String> split(String string, String separator)
  {
    List<String> splits = Lists.newArrayList();
    int index = 0;
    while (index < string.length()) {
      char quote = checkQuote(string, index);
      if (quote != 0x00) {
        // todo handle escaped quote
        int quoteEnd = string.indexOf(quote, index + 1);
        if (quoteEnd > 0 && string.substring(quoteEnd + 1).startsWith(separator)) {
          splits.add(string.substring(index + 1, quoteEnd));
          index = quoteEnd + 1 + separator.length();
          continue;
        }
        // ignore quote
      }
      int splitEnd = string.indexOf(separator, index);
      if (splitEnd < 0) {
        splits.add(string.substring(index, string.length()));
        break;
      }
      splits.add(string.substring(index, splitEnd));
      index = splitEnd + separator.length();
    }
    return splits;
  }

  private static char checkQuote(String string, int index)
  {
    // handle white space?
    char escape = 0x00;
    char c = string.charAt(index);
    if (c == '\'' || c == '"') {
      escape = c;
    }
    return escape;
  }
}
