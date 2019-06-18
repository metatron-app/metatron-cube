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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.collect.Utils;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;

import java.util.List;
import java.util.Map;

// copied from com.metamx.common.parsers.CSVParser
public class CSVParser implements Parser<String, Object>
{
  private static final Function<String, String> EMPTY_TO_NULL = ParserUtils.nullEmptyStringFunction;
  private static final Function<String, String> TRIM = new Function<String, String>()
  {
    @Override
    public String apply(String input)
    {
      return input != null ? input.trim() : input;
    }
  };

  public static Function<String, String> getStringHandler(boolean trim)
  {
    return trim ? Functions.compose(EMPTY_TO_NULL, TRIM) : EMPTY_TO_NULL;
  }

  private final Splitter listSplitter;
  private final Function<String, Object> valueFunction;

  private final au.com.bytecode.opencsv.CSVParser parser = new au.com.bytecode.opencsv.CSVParser();

  private final List<String> fieldNames;

  public CSVParser(final String listDelimiter, final List<String> fieldNames, final boolean trim)
  {
    this.fieldNames = fieldNames;
    this.listSplitter = listDelimiter == null ? null : Splitter.on(listDelimiter);

    final Function<String, String> function = getStringHandler(trim);
    this.valueFunction = new Function<String, Object>()
    {
      @Override
      public Object apply(String input)
      {
        if (listDelimiter != null && input.contains(listDelimiter)) {
          return Lists.newArrayList(
              Iterables.transform(listSplitter.split(input), function)
          );
        } else {
          return function.apply(input);
        }
      }
    };
  }

  @Override
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @Override
  public void setFieldNames(final Iterable<String> fieldNames)
  {
    throw new UnsupportedOperationException("setFieldNames");
  }

  @Override
  public Map<String, Object> parse(final String input)
  {
    try {
      String[] values = parser.parseLine(input);
      return Utils.zipMapPartial(fieldNames, Iterables.transform(Lists.newArrayList(values), valueFunction));
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }
}
