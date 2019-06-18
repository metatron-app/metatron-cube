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

package io.druid.data.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 */
public class DimensionsSpecTest
{
  @Test
  public void test() throws IOException
  {
    ObjectMapper mapper = TestHelper.getObjectMapper();
    String spec1 = "{\"dimensions\": [\"dim1\", \"dim2\"] }";
    Assert.assertEquals(
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2")), null, null),
        mapper.readValue(spec1, DimensionsSpec.class)
    );
    String spec2 = "{\"dimensions\": [{\"name\": \"dim1\", \"multiValueHandling\": \"SET\"}, \"dim2\"] }";
    Assert.assertEquals(
        new DimensionsSpec(
            Arrays.<DimensionSchema>asList(
                new StringDimensionSchema("dim1", DimensionSchema.MultiValueHandling.SET),
                new StringDimensionSchema("dim2", DimensionSchema.MultiValueHandling.ARRAY)
            ), null, null
        ),
        mapper.readValue(spec2, DimensionsSpec.class)
    );
    String spec3 = "{\"dimensions\": [{\"name\": \"dim1\", \"multiValueHandling\":\"SET\"}, \"dim2\"] }";
    Assert.assertEquals(
        new DimensionsSpec(
            Arrays.<DimensionSchema>asList(
                new StringDimensionSchema("dim1", DimensionSchema.MultiValueHandling.SET),
                new StringDimensionSchema("dim2", DimensionSchema.MultiValueHandling.ARRAY)
            ), null, null
        ),
        mapper.readValue(spec3, DimensionsSpec.class)
    );
  }
}
