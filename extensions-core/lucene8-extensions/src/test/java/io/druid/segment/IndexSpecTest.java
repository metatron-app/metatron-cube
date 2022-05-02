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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.segment.lucene.LatLonPointIndexingStrategy;
import io.druid.segment.lucene.TextIndexingStrategy;
import io.druid.segment.lucene.Lucene8IndexingSpec;
import org.junit.Assert;
import org.junit.Test;

public class IndexSpecTest
{
  @Test
  public void testSecondaryIndexing() throws Exception
  {
    final ObjectMapper mapper = Lucene8TestHelper.segmentWalker.getMapper();
    final IndexSpec spec = new IndexSpec(
        null, null, null, null, null,
        ImmutableMap.<String, SecondaryIndexingSpec>of(
            "gis", Lucene8IndexingSpec.of(
                "standard",
                new LatLonPointIndexingStrategy("coord", "lat", "lon", null),
                new TextIndexingStrategy("text")
            )
        ),
        null,
        false,
        null
    );
    Assert.assertEquals(
        "{\"bitmap\":{\"type\":\"roaring\"},\"secondaryIndexing\":{\"gis\":{\"type\":\"lucene8\",\"textAnalyzer\":\"standard\",\"strategies\":[{\"type\":\"latlon\",\"fieldName\":\"coord\",\"latitude\":\"lat\",\"longitude\":\"lon\",\"crs\":null},{\"type\":\"text\",\"fieldName\":\"text\"}]}},\"allowNullForNumbers\":false}",
        mapper.writeValueAsString(spec)
    );
    IndexSpec actual = mapper.readValue(mapper.writeValueAsBytes(spec), IndexSpec.class);
    Assert.assertEquals(spec, actual);
  }
}
