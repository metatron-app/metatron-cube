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

package io.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.Lucene7TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class LucenePointFilterSerDeTest
{
  @Test
  public void test() throws Exception
  {
    ObjectMapper mapper = Lucene7TestHelper.segmentWalker.getMapper();
    LucenePointFilter filter = LucenePointFilter.distance("field", 37, 120, 10000);
    String serialized = mapper.writeValueAsString(filter);
    Assert.assertEquals(
        "{\"type\":\"lucene.point\",\"field\":\"field\",\"query\":\"distance\",\"latitudes\":[37.0],\"longitudes\":[120.0],\"radiusMeters\":10000.0}",
        serialized
    );
    Assert.assertEquals(filter, mapper.readValue(serialized, DimFilter.class));
  }
}