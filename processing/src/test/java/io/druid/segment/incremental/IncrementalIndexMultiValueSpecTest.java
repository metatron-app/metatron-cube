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

package io.druid.segment.incremental;

import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class IncrementalIndexMultiValueSpecTest
{
  @Test
  public void test() throws IndexSizeExceededException
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.<DimensionSchema>asList(
            new StringDimensionSchema("string1", DimensionSchema.MultiValueHandling.ARRAY, -1),
            new StringDimensionSchema("string2", DimensionSchema.MultiValueHandling.SORTED_ARRAY, -1),
            new StringDimensionSchema("string3", DimensionSchema.MultiValueHandling.SET, -1),
            new FloatDimensionSchema("float1", DimensionSchema.MultiValueHandling.ARRAY),
            new FloatDimensionSchema("float2", DimensionSchema.MultiValueHandling.SORTED_ARRAY),
            new FloatDimensionSchema("float3", DimensionSchema.MultiValueHandling.SET),
            new LongDimensionSchema("long1", DimensionSchema.MultiValueHandling.ARRAY),
            new LongDimensionSchema("long2", DimensionSchema.MultiValueHandling.SORTED_ARRAY),
            new LongDimensionSchema("long3", DimensionSchema.MultiValueHandling.SET)
        ),
        null, null
    );
    IncrementalIndexSchema schema = new IncrementalIndexSchema(
        0,
        QueryGranularities.ALL,
        dimensionsSpec,
        new AggregatorFactory[0]
    );
    Map<String, Object> map = new HashMap<String, Object>()
    {
      @Override
      public Object get(Object key)
      {
        if (((String) key).startsWith("string")) {
          return Arrays.asList("xsd", "aba", "fds", "aba");
        }
        if (((String) key).startsWith("float")) {
          return Arrays.<Float>asList(3.92f, -2.76f, 42.153f, Float.NaN, -2.76f, -2.76f);
        }
        if (((String) key).startsWith("long")) {
          return Arrays.<Long>asList(-231238789L, 328L, 923L, 328L, -2L, 0L);
        }
        return null;
      }
    };
    IncrementalIndex<?> index = new OnheapIncrementalIndex(schema, true, 10000);
    index.add(
        new MapBasedInputRow(
            0, Arrays.asList(
            "string1", "string2", "string3", "float1", "float2", "float3", "long1", "long2", "long3"
        ), map
        )
    );

    Row row = index.iterator().next();
    Assert.assertArrayEquals(new Object[] {"xsd", "aba", "fds", "aba"}, (Object[])row.getRaw("string1"));
    Assert.assertArrayEquals(new Object[] {"aba", "aba", "fds", "xsd"}, (Object[])row.getRaw("string2"));
    Assert.assertArrayEquals(new Object[] {"aba", "fds", "xsd"}, (Object[])row.getRaw("string3"));

    Assert.assertArrayEquals(new Float[] {3.92f, -2.76f, 42.153f, Float.NaN, -2.76f, -2.76f}, (Object[])row.getRaw("float1"));
    Assert.assertArrayEquals(new Float[] {-2.76f, -2.76f, -2.76f, 3.92f, 42.153f, Float.NaN}, (Object[])row.getRaw("float2"));
    Assert.assertArrayEquals(new Float[] {-2.76f, 3.92f, 42.153f, Float.NaN}, (Object[])row.getRaw("float3"));

    Assert.assertArrayEquals(new Long[] {-231238789L, 328L, 923L, 328L, -2L, 0L}, (Object[])row.getRaw("long1"));
    Assert.assertArrayEquals(new Long[] {-231238789L, -2L, 0L, 328L, 328L, 923L}, (Object[])row.getRaw("long2"));
    Assert.assertArrayEquals(new Long[] {-231238789L, -2L, 0L, 328L, 923L}, (Object[])row.getRaw("long3"));
  }
}