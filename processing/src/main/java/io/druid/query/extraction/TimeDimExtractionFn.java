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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.JodaUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.util.Objects;

/**
 */
public class TimeDimExtractionFn extends DimExtractionFn implements ExtractionFn.Stateful
{
  private final String timeFormat;
  private final String timeLocale;
  private final String timeZone;
  private final DateTimeFormatter timeFormatter;

  private final String resultFormat;
  private final String resultLocale;
  private final String resultZone;
  private final DateTimeFormatter resultFormatter;

  @JsonCreator
  public TimeDimExtractionFn(
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("timeLocale") String timeLocale,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("resultFormat") String resultFormat,
      @JsonProperty("resultLocale") String resultLocale,
      @JsonProperty("resultZone") String resultZone
  )
  {
    Preconditions.checkNotNull(timeFormat, "timeFormat must not be null");
    Preconditions.checkNotNull(resultFormat, "resultFormat must not be null");

    this.timeFormat = timeFormat;
    this.timeLocale = timeLocale;
    this.timeZone = timeZone;
    this.timeFormatter = JodaUtils.toTimeFormatter(timeFormat, timeZone, timeLocale);

    this.resultFormat = resultFormat;
    this.resultLocale = resultLocale;
    this.resultZone = resultZone;
    this.resultFormatter = JodaUtils.toTimeFormatter(resultFormat, resultZone, resultLocale);
  }

  public TimeDimExtractionFn(String timeFormat, String resultFormat)
  {
    this(timeFormat, null, null, resultFormat, null, null);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_DIM)
                  .append(timeFormat).sp()
                  .append(timeLocale).sp()
                  .append(timeZone).sp()
                  .append(resultFormat).sp()
                  .append(resultLocale).sp()
                  .append(resultZone);
  }

  @Override
  public String apply(String dimValue)
  {
    DateTime date;
    try {
      date = DateTime.parse(dimValue, timeFormatter);
    }
    catch (IllegalArgumentException e) {
      return dimValue;
    }
    return resultFormatter.print(date);
  }

  @JsonProperty("timeFormat")
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @JsonProperty("timeLocale")
  public String getTimeLocale()
  {
    return timeLocale;
  }

  @JsonProperty("timeZone")
  public String getTimeZone()
  {
    return timeZone;
  }

  @JsonProperty("resultFormat")
  public String getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty("resultLocale")
  public String getResultLocale()
  {
    return resultLocale;
  }

  @JsonProperty("resultZone")
  public String getResultZone()
  {
    return resultZone;
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
  public ExtractionFn init()
  {
    return new TimeDimExtractionFn(timeFormat, timeLocale, timeZone, resultFormat, resultLocale, resultZone);
  }

  @Override
  public int arity()
  {
    return 1;
  }

  @Override
  public String toString()
  {
    return "TimeDimExtractionFn{" +
           "timeFormat='" + timeFormat + '\'' +
           (timeLocale != null ? ", timeLocale='" + timeLocale + '\'' : "") +
           (timeZone != null ? ", timeZone='" + timeZone + '\'' : "") +
           ", resultFormat='" + resultFormat + '\'' +
           (resultLocale != null ? ", resultLocale='" + resultLocale + '\'' : "") +
           (resultZone != null ? ", resultZone='" + resultZone + '\'' : "") +
           '}';
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

    TimeDimExtractionFn that = (TimeDimExtractionFn) o;

    if (!timeFormat.equals(that.timeFormat)) {
      return false;
    }
    if (!Objects.equals(timeLocale, that.timeLocale)) {
      return false;
    }
    if (!Objects.equals(timeZone, that.timeZone)) {
      return false;
    }
    if (!resultFormat.equals(that.resultFormat)) {
      return false;
    }
    if (!Objects.equals(resultLocale, that.resultLocale)) {
      return false;
    }
    if (!Objects.equals(resultZone, that.resultZone)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(timeFormat, timeLocale, timeZone, resultFormat, resultLocale, resultZone);
  }
}
