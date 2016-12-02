/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.ibm.icu.text.SimpleDateFormat;
import com.ibm.icu.util.TimeZone;
import io.druid.common.utils.StringUtils;
import io.druid.query.aggregation.AggregatorUtil;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Objects;

public class TimeFormatExtractionFn implements ExtractionFn.Stateful
{
  private final TimeZone tz;
  private final String pattern;
  private final Locale locale;
  private final SimpleDateFormat formatter;

  public TimeFormatExtractionFn(
      @JsonProperty("format") String pattern,
      @JsonProperty("timeZone") String tzString,
      @JsonProperty("locale") String localeString
  )
  {
    this(
        pattern,
        tzString == null ? null : TimeZone.getTimeZone(tzString),
        localeString == null ? null : Locale.forLanguageTag(localeString)
    );
  }

  private TimeFormatExtractionFn(String pattern, TimeZone tz, Locale locale) {
    this.pattern = Preconditions.checkNotNull(pattern, "format cannot be null");
    this.tz = tz;
    this.locale = locale;
    this.formatter = locale == null ? new SimpleDateFormat(pattern) : new SimpleDateFormat(pattern, locale);
    this.formatter.setTimeZone(tz == null ? TimeZone.getTimeZone("UTC") : tz);
  }

  @JsonProperty
  public String getTimeZone()
  {
    return tz == null ? null : tz.getID();
  }

  @JsonProperty
  public String getFormat()
  {
    return pattern;
  }

  @JsonProperty
  public String getLocale()
  {
    return locale == null ? null : locale.toLanguageTag();
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] patternBytes = StringUtils.toUtf8(pattern);
    byte[] timeZoneBytes = StringUtils.toUtf8WithNullToEmpty(getTimeZone());
    byte[] localeBytes = StringUtils.toUtf8WithNullToEmpty(getLocale());
    return ByteBuffer.allocate(3 + patternBytes.length + timeZoneBytes.length + localeBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_FORMAT)
                     .put(patternBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(timeZoneBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(localeBytes)
                     .array();
  }

  @Override
  public ExtractionFn init()
  {
    return new TimeFormatExtractionFn(pattern, tz, locale);
  }

  @Override
  public String apply(long value)
  {
    return formatter.format(value);
  }

  @Override
  public String apply(Object value)
  {
    return formatter.format(new DateTime(value).getMillis());
  }

  @Override
  public String apply(String value)
  {
    return apply((Object) value);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public int arity()
  {
    return 1;
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

    TimeFormatExtractionFn that = (TimeFormatExtractionFn) o;

    if (!Objects.equals(getLocale(), that.getLocale())) {
      return false;
    }
    if (!Objects.equals(getTimeZone(), that.getTimeZone())) {
      return false;
    }
    if (!pattern.equals(that.pattern)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getLocale(), getTimeZone(), pattern);
  }

  @Override
  public String toString()
  {
    return "TimeFormatExtractionFn{" +
           "tz=" + getTimeZone() +
           ", pattern='" + getFormat() + '\'' +
           ", locale=" + getLocale() +
           '}';
  }
}
