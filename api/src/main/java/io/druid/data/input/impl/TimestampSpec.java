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
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParserUtils;
import com.metamx.common.parsers.TimestampParser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 */
public class TimestampSpec
{
  private static final Logger log = new Logger(TimestampSpec.class);

  private static class ParseCtx
  {
    Object lastTimeObject = null;
    DateTime lastDateTime = null;
  }

  private static final String DEFAULT_COLUMN = "timestamp";
  private static final String DEFAULT_FORMAT = "auto";
  private static final DateTime DEFAULT_MISSING_VALUE = null;

  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<Object, DateTime> timestampConverter;
  // this value should never be set for production data
  private final DateTime missingValue;

  // remember last value parsed
  private final ParseCtx parseCtx = new ParseCtx();

  @Inject
  private static Properties properties;

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue
  )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.timestampConverter = createObjectTimestampParser(timestampFormat);
    this.missingValue = missingValue == null
                        ? DEFAULT_MISSING_VALUE
                        : missingValue;
  }

  private Function<Object, DateTime> createObjectTimestampParser(String format)
  {
    if (!"adaptive".equalsIgnoreCase(format)) {
      return TimestampParser.createObjectTimestampParser(format);
    }
    final Function<String, Function<String, DateTime>> supplier = new Function<String, Function<String, DateTime>>()
    {
      @Override
      public Function<String, DateTime> apply(String input)
      {
        String property = properties.getProperty("adaptive.timestamp.format.list");
        if (property != null && property.startsWith("[") && property.endsWith("]")) {
          property = property.substring(1, property.length() - 1);
        }
        if (!Strings.isNullOrEmpty(property)) {
          return findFormat(input, property.split(","));
        } else {
          return findFormat(input);
        }
      }
    };
    final Function<Number, DateTime> numericFunc = TimestampParser.createNumericTimestampParser(timestampFormat);
    return new Function<Object, DateTime>()
    {
      private Function<String, DateTime> stringFunc;
      @Override
      public DateTime apply(Object input)
      {
        if (input instanceof Number) {
          return numericFunc.apply((Number) input);
        }
        String string = String.valueOf(input);
        if (stringFunc == null) {
          stringFunc = supplier.apply(string);
        }
        return stringFunc.apply(string);
      }
    };
  }

  private Function<String, DateTime> findFormat(String input, String... formats)
  {
    log.info("finding format with candidates.. " + Arrays.toString(formats));
    String strip = ParserUtils.stripQuotes(input);
    for (String knownFormat : formats) {
      final DateTimeFormatter formatter = DateTimeFormat.forPattern(ParserUtils.stripQuotes(knownFormat.trim()));
      try {
        DateTime t = formatter.parseDateTime(strip);
        log.info("applied '" + knownFormat + "' format to " + input + " and acquired " + t);
        return new Function<String, DateTime>()
        {
          @Override
          public DateTime apply(String input)
          {
            return formatter.parseDateTime(ParserUtils.stripQuotes(input));
          }
        };
      }
      catch (Exception e) {
        // failed.. try next
      }
    }
    try {
      DateTime t = new DateTime(strip);
      log.info("applied iso format to " + input + " and acquired " + t);
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          return new DateTime(ParserUtils.stripQuotes(input));
        }
      };
    }
    catch (Exception e) {
      // ignore.. not iso
    }
    if (isStringLong(input)) {
      Function<String, DateTime> function =
          Functions.compose(TimestampParser.createNumericTimestampParser(timestampFormat), Longs.stringConverter());
      try {
        DateTime t = function.apply(input);
        log.info("regarded " + input + " as unix timestamp and acquired " + t);
        return function;
      }
      catch (Exception e) {
        // ignore.. not timestamp
      }
    }
    log.info("failed to find appropriate format.");
    return TimestampParser.createTimestampParser("auto");
  }

  private static boolean isStringLong(String input)
  {
    for (int i = 0; i < input.length(); i++) {
      if (!Character.isDigit(input.charAt(i))) {
        return false;
      }
    }
    try {
      Long.parseLong(input);
      return true;
    }
    catch (NumberFormatException e) {
      return false;
    }
  }

  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty("format")
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  @JsonProperty("missingValue")
  public DateTime getMissingValue()
  {
    return missingValue;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = input.get(timestampColumn);
    return parseDateTime(o);
  }

  public DateTime parseDateTime(Object input)
  {
    DateTime extracted = null;
    if (input != null) {
      if (input.equals(parseCtx.lastTimeObject)) {
        extracted = parseCtx.lastDateTime;
      } else {
        parseCtx.lastTimeObject = input;
        parseCtx.lastDateTime = extracted = timestampConverter.apply(input);
      }
    }
    return extracted == null ? missingValue : extracted;
  }

  public Timestamp parseTimestamp(Object input)
  {
    DateTime dateTime = parseDateTime(input);
    if (dateTime != null) {
      return new Timestamp(dateTime.getMillis());
    }

    return null;
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

    TimestampSpec that = (TimestampSpec) o;

    if (!timestampColumn.equals(that.timestampColumn)) {
      return false;
    }
    if (!timestampFormat.equals(that.timestampFormat)) {
      return false;
    }
    return !(missingValue != null ? !missingValue.equals(that.missingValue) : that.missingValue != null);

  }

  @Override
  public int hashCode()
  {
    int result = timestampColumn.hashCode();
    result = 31 * result + timestampFormat.hashCode();
    result = 31 * result + (missingValue != null ? missingValue.hashCode() : 0);
    return result;
  }

  public static void main(String[] args)
  {
    System.out.println("[TimestampSpec/main] " + new DateTime("2015-09-12T00:46:58.771Z"));
  }
}
