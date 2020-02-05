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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.data.input.TimestampSpec;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

public class TimestampSpecTest
{
  @Test
  public void testExtractTimestamp() throws Exception
  {
    TimestampSpec spec = new DefaultTimestampSpec("TIMEstamp", "yyyy-MM-dd", null);
    Assert.assertEquals(
        new DateTime("2014-03-01"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("TIMEstamp", "2014-03-01"))
    );
    spec = new DefaultTimestampSpec("ts", "yyyy/MM/dd HH:mm:ss.SSS", null);
    Assert.assertEquals(
        new DateTime("2002-01-01T03:44:14.50"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("ts", "2002/01/01 03:44:14.50"))
    );
    spec = new DefaultTimestampSpec("ts", "yyyy/MM/dd HH:mm:ss,SSS", null);
    Assert.assertEquals(
        new DateTime("2002-01-01T03:44:14.50"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("ts", "2002/01/01 03:44:14,50"))
    );
    spec = new DefaultTimestampSpec("ts", "yy-MM-dd a hh:mm", null).withTimeZone("Asia/Seoul")
                                                                   .withLocale(Locale.KOREA.getLanguage());
    Assert.assertEquals(
        new DateTime("2019-12-03T06:33:00.000", DateTimeZone.forID("Asia/Seoul")),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("ts", "19-12-3 오전 6:33"))
    );

    DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss.SSSSSS a");
    formatter = formatter.withLocale(new Locale("en"));
    Assert.assertEquals("01/01/2002 11:44:14.500000 PM", formatter.print(new DateTime("2002-01-01T23:44:14.50")));
    Assert.assertEquals(new DateTime("2012-10-01T22:31:00.881Z"), formatter.parseDateTime("10/1/2012 10:31:00.881000 PM"));
  }

  @Test
  public void testExtractTimestampWithMissingTimestampColumn() throws Exception
  {
    TimestampSpec spec = new DefaultTimestampSpec(null, null, new DateTime(0));
    Assert.assertEquals(
        new DateTime("1970-01-01"),
        spec.extractTimestamp(ImmutableMap.<String, Object>of("dim", "foo"))
    );
  }

  @Test
  public void testContextualTimestampList() throws Exception
  {
    String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    String[] dates = new String[]{
        "2000-01-01T05:00:00",
        "2000-01-01T05:00:01",
        "2000-01-01T05:00:01",
        "2000-01-01T05:00:02",
        "2000-01-01T05:00:03",
        };
    TimestampSpec spec = new DefaultTimestampSpec("TIMEstamp", DATE_FORMAT, null);

    for (int i = 0; i < dates.length; ++i) {
      String date = dates[i];
      DateTime dateTime = spec.extractTimestamp(ImmutableMap.<String, Object>of("TIMEstamp", date));
      DateTime expectedDateTime = ISODateTimeFormat.dateHourMinuteSecond().parseDateTime(date);
      Assert.assertEquals(expectedDateTime, dateTime);
    }

    spec = new DefaultTimestampSpec("TIMEstamp", DATE_FORMAT, null, null, false, false, "Asia/Saigon", null);
    for (int i = 0; i < dates.length; ++i) {
      String date = dates[i];
      DateTime dateTime = spec.extractTimestamp(ImmutableMap.<String, Object>of("TIMEstamp", date));
      Assert.assertTrue(dateTime.toString().endsWith("+07:00"));
    }
  }

  @Test
  public void testInvalidOrMissingTimestamp() throws Exception
  {
    DateTime missing = new DateTime(9998);
    DateTime invalid = new DateTime(9999);
    TimestampSpec spec = new DefaultTimestampSpec("TIMEstamp", "yyyy-MM-dd", missing, invalid, true, false, null, null);
    Map<String, Object> invalidRow = Maps.newHashMap(ImmutableMap.<String, Object>of("TIMEstamp", "2014-03-XY"));
    Map<String, Object> missingRow = Maps.newHashMap();
    Assert.assertEquals(invalid, spec.extractTimestamp(invalidRow));
    Assert.assertEquals(missing, spec.extractTimestamp(missingRow));

    Assert.assertEquals(invalid.toString(), invalidRow.get("TIMEstamp"));
    Assert.assertEquals(missing.toString(), missingRow.get("TIMEstamp"));
  }
}
