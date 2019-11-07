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
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.lucene.LatLonPointIndexingStrategy;
import io.druid.segment.lucene.LuceneIndexingSpec;
import io.druid.segment.lucene.TextIndexingStrategy;
import org.junit.Assert;
import org.junit.Test;

public class IndexSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"bitmap\" : { \"type\" : \"roaring\" }, \"dimensionCompression\" : \"lz4\", \"metricCompression\" : \"lzf\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);
    Assert.assertEquals(new RoaringBitmapSerdeFactory(), spec.getBitmapSerdeFactory());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZ4, spec.getDimensionCompressionStrategy());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZF, spec.getMetricCompressionStrategy());

    Assert.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testSerdeUncompressed() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"dimensionCompression\" : \"uncompressed\" }";

    final IndexSpec spec = objectMapper.readValue(json, IndexSpec.class);

    Assert.assertEquals(IndexSpec.UNCOMPRESSED, spec.getDimensionCompression());
    Assert.assertEquals(null, spec.getDimensionCompressionStrategy());
    Assert.assertEquals(spec, objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class));
  }

  @Test
  public void testDefaults() throws Exception
  {
    final IndexSpec spec = new IndexSpec();
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZ4, spec.getDimensionCompressionStrategy());
    Assert.assertEquals(CompressedObjectStrategy.CompressionStrategy.LZ4, spec.getMetricCompressionStrategy());
  }

  @Test
  public void testSecondaryIndexing() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final IndexSpec spec = new IndexSpec(
        null, null, null, null,
        ImmutableMap.<String, SecondaryIndexingSpec>of(
            "gis", LuceneIndexingSpec.of(
                "standard",
                new LatLonPointIndexingStrategy("coord", "lat", "lon", null),
                new TextIndexingStrategy("text")
            )
        ),
        null,
        false
    );
    IndexSpec actual = objectMapper.readValue(objectMapper.writeValueAsBytes(spec), IndexSpec.class);
    Assert.assertEquals(spec, actual);
  }
}
