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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.ibm.icu.text.SimpleDateFormat;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.query.aggregation.AggregatorUtil;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.nio.ByteBuffer;

/**
 */
public class TimeDimExtractionFn extends DimExtractionFn implements ExtractionFn.Stateful
{
  private final String timeFormat;
  private final DateTimeFormatter timeFormatter;
  private final String resultFormat;
  private final SimpleDateFormat resultFormatter;

  @JsonCreator
  public TimeDimExtractionFn(
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("resultFormat") String resultFormat
  )
  {
    Preconditions.checkNotNull(timeFormat, "timeFormat must not be null");
    Preconditions.checkNotNull(resultFormat, "resultFormat must not be null");

    this.timeFormat = timeFormat;
    this.timeFormatter = JodaUtils.toTimeFormatter(timeFormat);

    this.resultFormat = resultFormat;
    this.resultFormatter = new SimpleDateFormat(resultFormat);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] timeFormatBytes = StringUtils.toUtf8(timeFormat);
    byte[] resultFormatBytes = StringUtils.toUtf8(resultFormat);
    return ByteBuffer.allocate(2 + timeFormatBytes.length + resultFormatBytes.length)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_TIME_DIM)
                     .put(timeFormatBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(resultFormatBytes)
                     .array();
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
    return resultFormatter.format(date.getMillis());
  }

  @JsonProperty("timeFormat")
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @JsonProperty("resultFormat")
  public String getResultFormat()
  {
    return resultFormat;
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
    return new TimeDimExtractionFn(timeFormat, resultFormat);
  }

  @Override
  public String toString()
  {
    return "TimeDimExtractionFn{" +
           "timeFormat='" + timeFormat + '\'' +
           ", resultFormat='" + resultFormat + '\'' +
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

    if (!resultFormat.equals(that.resultFormat)) {
      return false;
    }
    if (!timeFormat.equals(that.timeFormat)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = timeFormat.hashCode();
    result = 31 * result + resultFormat.hashCode();
    return result;
  }
}
