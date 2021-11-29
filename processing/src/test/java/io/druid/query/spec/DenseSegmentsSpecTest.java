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

package io.druid.query.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.JacksonModule;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DenseSegmentsSpecTest
{
  private final ObjectMapper mapper = new JacksonModule().smileMapper();

  @Test
  public void testSerDe() throws Exception
  {
    Interval interval1 = Interval.parse("1992-01-01/1992-02-01");
    Interval interval2 = Interval.parse("1992-02-01/1992-03-01");
    Interval interval3 = Interval.parse("1992-03-01/1992-04-01");
    SegmentDescriptor desc1 = new SegmentDescriptor("ds", interval1, "100", 1);
    SegmentDescriptor desc2 = new SegmentDescriptor("ds", interval1, "100", 3);
    SegmentDescriptor desc3 = new SegmentDescriptor("ds", interval1, "100", 5);
    SegmentDescriptor desc4 = new SegmentDescriptor("ds", interval2, "200", 1);
    SegmentDescriptor desc5 = new SegmentDescriptor("ds", interval3, "300", 1);
    SegmentDescriptor desc6 = new SegmentDescriptor("ds", interval3, "300", 2);

    List<SegmentDescriptor> descs = Arrays.asList(desc1, desc2, desc3, desc4, desc5, desc6);
    QuerySegmentSpec spec1 = new MultipleSpecificSegmentSpec(descs);
    byte[] b1 = mapper.writeValueAsBytes(spec1);
    System.out.println(b1.length);
    Assert.assertEquals(spec1, mapper.readValue(b1, QuerySegmentSpec.class));

    QuerySegmentSpec spec2 = DenseSegmentsSpec.of("ds", descs);
    byte[] b2 = mapper.writeValueAsBytes(spec2);
    System.out.println(b2.length);
    Assert.assertEquals(spec2, mapper.readValue(b2, QuerySegmentSpec.class));
  }
}
