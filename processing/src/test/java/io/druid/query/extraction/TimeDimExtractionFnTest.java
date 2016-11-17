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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.utils.JodaUtils;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 */
public class TimeDimExtractionFnTest
{
  private static final String[] dims = {
      "01/01/2012",
      "01/02/2012",
      "03/03/2012",
      "03/04/2012",
      "05/05/2012",
      "12/21/2012"
  };

  @Test
  public void testMonthExtraction()
  {
    Set<String> months = Sets.newHashSet();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "MM/yyyy");

    for (String dim : dims) {
      months.add(extractionFn.apply(dim));
    }

    Assert.assertEquals(months.size(), 4);
    Assert.assertTrue(months.contains("01/2012"));
    Assert.assertTrue(months.contains("03/2012"));
    Assert.assertTrue(months.contains("05/2012"));
    Assert.assertTrue(months.contains("12/2012"));
  }

  @Test
  public void testQuarterExtraction()
  {
    Set<String> quarters = Sets.newHashSet();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy", "QQQ/yyyy");

    for (String dim : dims) {
      quarters.add(extractionFn.apply(dim));
    }

    Assert.assertEquals(quarters.size(), 3);
    Assert.assertTrue(quarters.contains("Q1/2012"));
    Assert.assertTrue(quarters.contains("Q2/2012"));
    Assert.assertTrue(quarters.contains("Q4/2012"));
  }

  @Test
  public void testOptionalExtraction()
  {
    List<String> results = Lists.newArrayList();
    ExtractionFn extractionFn = new TimeDimExtractionFn("MM/dd/yyyy[ HH:mm:ss]", "QQQ/yyyy MM/dd HH:mm:ss");

    for (String dim : new String[] {"01/12/2016", "06/24/2014", "08/30/2016 22:17:12", "12/03/2015 01:59:00"}) {
      results.add(extractionFn.apply(dim));
    }

    Assert.assertEquals("Q1/2016 01/12 00:00:00", results.get(0));
    Assert.assertEquals("Q2/2014 06/24 00:00:00", results.get(1));
    Assert.assertEquals("Q3/2016 08/30 22:17:12", results.get(2));
    Assert.assertEquals("Q4/2015 12/03 01:59:00", results.get(3));
  }

  @Test
  public void testWithLocaleZoneExtraction()
  {
    long time = 1479345099662L;
    String korean = Locale.KOREA.getLanguage();
    String us = Locale.US.getLanguage();
    DateTimeFormatter formatter = JodaUtils.toTimeFormatter("yyyy-MM-dd a hh:mm:ss.SSSZZ", korean, "+0900");
    String dateString = formatter.print(time);
    Assert.assertEquals("2016-11-17 오전 10:11:39.662+09:00", dateString);

    ExtractionFn extractionFn = new TimeDimExtractionFn(
        "yyyy-MM-dd a hh:mm:ss.SSSZZ", korean, "+0900",
        "yyyy-MM-dd a hh:mm:ss.SSSZZ", us, "PST");
    extractionFn.getCacheKey();

    Assert.assertEquals("2016-11-16 PM 05:11:39.662-0800", extractionFn.apply(dateString));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"time\", \"timeFormat\" : \"MM/dd/yyyy\", \"resultFormat\" : \"QQQ/yyyy\" }";
    TimeDimExtractionFn extractionFn = (TimeDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("MM/dd/yyyy", extractionFn.getTimeFormat());
    Assert.assertEquals("QQQ/yyyy", extractionFn.getResultFormat());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
