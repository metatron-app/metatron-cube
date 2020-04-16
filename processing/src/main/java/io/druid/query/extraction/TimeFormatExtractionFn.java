/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

public class TimeFormatExtractionFn implements ExtractionFn.Stateful
{
  private final String timeZone;
  private final String pattern;
  private final String locale;
  private final DateTimeFormatter formatter;
  private final Granularity granularity;

  public TimeFormatExtractionFn(
      @JsonProperty("format") String pattern,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("locale") String locale,
      @JsonProperty("granularity") Granularity granularity
  )
  {
    this.pattern = pattern;
    this.timeZone = timeZone;
    this.locale = locale;
    if (pattern != null) {
      this.formatter = JodaUtils.toTimeFormatter(pattern, timeZone, locale);
    } else {
      this.formatter = null;
    }
    this.granularity = granularity == null ? Granularities.NONE : granularity;
  }

  public TimeFormatExtractionFn(String format, String timeZone, String locale)
  {
    this(format, timeZone, locale, null);
  }

  @JsonProperty
  public String getTimeZone()
  {
    return timeZone;
  }

  @JsonProperty
  public String getFormat()
  {
    return pattern;
  }

  @JsonProperty
  public String getLocale()
  {
    return locale;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_FORMAT)
                  .append(pattern).sp()
                  .append(getTimeZone()).sp()
                  .append(getLocale()).sp()
                  .append(granularity);
  }

  @Override
  public ExtractionFn init()
  {
    return new TimeFormatExtractionFn(pattern, timeZone, locale, granularity);
  }

  @Override
  public String apply(long value)
  {
    long truncated = granularity.bucketStart(value);
    return formatter == null ? String.valueOf(truncated) : formatter.print(truncated);
  }

  @Override
  public String apply(Object value)
  {
    return apply(new DateTime(value).getMillis());
  }

  @Override
  public String apply(String value)
  {
    return apply((Object) value);
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
    if (!Objects.equals(getFormat(), that.getFormat())) {
      return false;
    }

    return granularity.equals(that.granularity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getLocale(), getTimeZone(), getFormat(), granularity);
  }

  @Override
  public String toString()
  {
    return "TimeFormatExtractionFn{" +
           "tz=" + getTimeZone() +
           ", pattern='" + getFormat() + '\'' +
           ", locale=" + getLocale() +
           ", granularity=" + getGranularity() +
           '}';
  }
}
