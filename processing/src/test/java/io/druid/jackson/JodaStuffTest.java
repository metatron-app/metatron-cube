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

package io.druid.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;

import java.io.IOException;
import java.util.Map;

public class JodaStuffTest extends TestCase
{
  public void testBasic() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    DateTime dateTime = new DateTime(10000000, DateTimeZone.forID("+09"));
    String serialized = mapper.writeValueAsString(dateTime);
    Assert.assertEquals("\"1970-01-01T11:46:40.000+09:00\"", serialized);
    DateTime deserialized = mapper.readValue(serialized, DateTime.class);
    Assert.assertEquals(dateTime, deserialized);

    DateTime before = ISODateTimeFormat.dateTimeParser().withOffsetParsed().parseDateTime(dateTime.toString());
    Assert.assertEquals(before, deserialized);

    ObjectMapper client = JodaStuff.overrideForInternal(mapper);
    serialized = client.writeValueAsString(dateTime);
    Assert.assertEquals("{\"m\":10000000,\"z\":\"+09:00\"}", serialized);
    deserialized = client.readValue(serialized, DateTime.class);
    Assert.assertEquals(dateTime, deserialized);
    Assert.assertEquals(dateTime, TimeBoundaryResultValue.getDateTimeValue(client.readValue(serialized, Map.class)));
  }

  public void testUTC() throws IOException
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    DateTime dateTime = new DateTime(10000000, DateTimeZone.forID("+00"));
    String serialized = mapper.writeValueAsString(dateTime);
    Assert.assertEquals("{\"m\":10000000}", serialized);
    DateTime deserialized = mapper.readValue(serialized, DateTime.class);
    Assert.assertEquals(dateTime, deserialized);

    DateTime before = ISODateTimeFormat.dateTimeParser().withOffsetParsed().parseDateTime(dateTime.toString());
    Assert.assertEquals(before, deserialized);
  }
}
