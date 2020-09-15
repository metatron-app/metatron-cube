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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.druid.data.input.TimestampSpec;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.TimestampParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 */
public class DefaultTimestampSpec implements TimestampSpec
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
  private final DateTime invalidValue;

  private final String missingValueString;
  private final String invalidValueString;
  private final DateTimeZone timeZone;
  private final String locale;

  private final boolean removeTimestampColumn;
  private final boolean replaceWrongColumn;

  // remember last value parsed
  private final ParseCtx parseCtx = new ParseCtx();

  @Inject
  static Properties properties;

  @JsonCreator
  public DefaultTimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue,
      @JsonProperty("invalidValue") DateTime invalidValue,
      @JsonProperty("replaceWrongColumn") boolean replaceWrongColumn,
      @JsonProperty("removeTimestampColumn") boolean removeTimestampColumn,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("locale") String locale
  )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.missingValue = missingValue == null
                        ? DEFAULT_MISSING_VALUE
                        : missingValue;
    this.invalidValue = invalidValue;
    this.missingValueString = this.missingValue == null ? null : this.missingValue.toString();
    this.invalidValueString = this.invalidValue == null ? null : this.invalidValue.toString();
    this.replaceWrongColumn = replaceWrongColumn && (missingValueString != null || invalidValueString != null);
    this.removeTimestampColumn = removeTimestampColumn;
    this.timestampConverter = createTimestampParser(timestampFormat);
    this.timeZone = timeZone == null ? null : DateTimeZone.forID(timeZone);
    this.locale = locale;
  }

  public DefaultTimestampSpec(
      String timestampColumn,
      String format,
      DateTime missingValue,
      DateTime invalidValue,
      boolean replaceWrongColumn,
      boolean removeTimestampColumn
  )
  {
    this(timestampColumn, format, missingValue, invalidValue, replaceWrongColumn, removeTimestampColumn, null, null);
  }

  public DefaultTimestampSpec(String timestampColumn, String format, DateTime missingValue)
  {
    this(timestampColumn, format, missingValue, null, false, false, null, null);
  }

  public DefaultTimestampSpec withTimeZone(String timeZone)
  {
    return new DefaultTimestampSpec(
        timestampColumn,
        timestampFormat,
        missingValue,
        invalidValue,
        replaceWrongColumn,
        removeTimestampColumn,
        timeZone,
        locale
    );
  }

  public DefaultTimestampSpec withLocale(String locale)
  {
    return new DefaultTimestampSpec(
        timestampColumn,
        timestampFormat,
        missingValue,
        invalidValue,
        replaceWrongColumn,
        removeTimestampColumn,
        timeZone == null ? null : timeZone.getID(),
        locale
    );
  }

  private <T> Function<T, DateTime> wrapInvalidHandling(final Function<T, DateTime> converter)
  {
    if (invalidValue == null) {
      return converter;
    }
    return new Function<T, DateTime>()
    {
      @Override
      public DateTime apply(T input)
      {
        try {
          return converter.apply(input);
        }
        catch (Exception e) {
          return invalidValue;
        }
      }
    };
  }

  private Function<Object, DateTime> createTimestampParser(String format)
  {
    final Function<Object, DateTime> delegate = createObjectTimestampParser(format);
    return new Function<Object, DateTime>()
    {
      @Override
      public DateTime apply(Object input)
      {
        return input instanceof DateTime ? ((DateTime) input) : delegate.apply(input);
      }
    };
  }

  private Function<Object, DateTime> createObjectTimestampParser(final String format)
  {
    final Function<String, Function<String, DateTime>> supplier = new Function<String, Function<String, DateTime>>()
    {
      @Override
      public Function<String, DateTime> apply(String input)
      {
        if (format.equals("adaptive") && properties != null) {
          String property = properties.getProperty("adaptive.timestamp.format.list", "").trim();
          if (property.startsWith("[") && property.endsWith("]")) {
            property = property.substring(1, property.length() - 1).trim();
          }
          return findFormat(stripQuotes(input), property.split(","));
        }
        return findFormat(stripQuotes(input), format);
      }
    };
    final Function<Number, DateTime> numericFunc = wrapInvalidHandling(
        TimestampParser.createNumericTimestampParser(timestampFormat)
    );
    return new Function<Object, DateTime>()
    {
      private Function<String, DateTime> stringFunc;

      @Override
      public DateTime apply(Object input)
      {
        if (input instanceof DateTime) {
          return (DateTime) input;
        }
        if (input instanceof Number) {
          return numericFunc.apply((Number) input);
        }
        String string = String.valueOf(input);
        if (stringFunc == null) {
          stringFunc = wrapInvalidHandling(supplier.apply(string));
        }
        return stringFunc.apply(string);
      }
    };
  }

  // removed "iso", which does not support timezone configuration
  private static final Set<String> BUILT_IN = ImmutableSet.of("auto", "posix", "millis", "nano", "ruby");

  private Function<String, DateTime> findFormat(String input, String... dateFormats)
  {
    DateTimeFormatter found = null;
    log.info("finding format with candidates.. " + Arrays.toString(dateFormats));
    for (String dateFormat : dateFormats) {
      if (BUILT_IN.contains(dateFormat)) {
        return TimestampParser.createTimestampParser(dateFormat);
      }
      DateTimeFormatter formatter;
      try {
        formatter = DateTimeFormat.forPattern(stripQuotes(dateFormat));
      }
      catch (Exception e) {
        log.info("Invalid format %s", dateFormat);
        continue;
      }
      found = isApplicable(input, formatter);
      if (found != null) {
        log.info("using format '%s'", dateFormat);
        break;
      }
    }
    if (found == null) {
      found = isApplicable(input, ISODateTimeFormat.dateTimeParser());
      if (found != null) {
        log.info("using iso format");
      }
    }
    if (found != null) {
      final DateTimeFormatter formatter = found;
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          return formatter.parseDateTime(stripQuotes(input));
        }
      };
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
    log.info("failed to find appropriate format for time '%s' in list.", input);
    return TimestampParser.createTimestampParser("auto");
  }

  private String stripQuotes(String input)
  {
    input = input.trim();
    if (input.length() > 0 && input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
      input = input.substring(1, input.length() - 1).trim();
    }
    return input;
  }

  private DateTimeFormatter isApplicable(String value, DateTimeFormatter formatter)
  {
    if (timeZone != null) {
      formatter = formatter.withZone(timeZone);
    }
    if (locale != null) {
      formatter = formatter.withLocale(new Locale(locale));
    }
    try {
      formatter.parseDateTime(value);
      return formatter;
    }
    catch (Exception e) {
      // failed.. try next
    }
    return null;
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

  @Override
  public List<String> getRequiredColumns()
  {
    return ImmutableList.of(timestampColumn);
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

  @JsonProperty("invalidValue")
  public DateTime getInvalidValue()
  {
    return invalidValue;
  }

  @JsonProperty("replaceWrongColumn")
  public boolean isReplaceWrongColumn()
  {
    return replaceWrongColumn;
  }

  @JsonProperty("timeZone")
  public String getTimeZone()
  {
    return timeZone == null ? null : timeZone.getID();
  }

  @JsonProperty("locale")
  public String getLocale()
  {
    return locale;
  }

  @Override
  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = removeTimestampColumn ? input.remove(timestampColumn) : input.get(timestampColumn);
    final DateTime extracted = parseDateTime(o);
    if (replaceWrongColumn && !removeTimestampColumn) {
      if (extracted == invalidValue) {
        input.put(timestampColumn, invalidValueString);
      } else {
        input.put(timestampColumn, missingValueString);
      }
    }
    return extracted;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultTimestampSpec that = (DefaultTimestampSpec) o;

    if (!timestampColumn.equals(that.timestampColumn)) {
      return false;
    }
    if (!timestampFormat.equals(that.timestampFormat)) {
      return false;
    }
    if (!Objects.equals(missingValue, that.missingValue)) {
      return false;
    }
    if (!Objects.equals(invalidValue, that.invalidValue)) {
      return false;
    }
    if (!Objects.equals(timeZone, that.timeZone)) {
      return false;
    }
    if (!Objects.equals(locale, that.locale)) {
      return false;
    }
    if (!Objects.equals(replaceWrongColumn, that.replaceWrongColumn)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = timestampColumn.hashCode();
    result = 31 * result + timestampFormat.hashCode();
    result = 31 * result + (missingValue != null ? missingValue.hashCode() : 0);
    result = 31 * result + (invalidValue != null ? invalidValue.hashCode() : 0);
    result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
    result = 31 * result + (locale != null ? locale.hashCode() : 0);
    result = 31 * result + (replaceWrongColumn ? 1 : 0);
    return result;
  }
}
