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

package io.druid.segment.incremental;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericMinAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class IncrementalIndexSchemaTest
{
  @Test
  public void testConvert()
  {
    Map<String, Object> ingestion = ImmutableMap.<String, Object>of(
        "dimensionsSpec", ImmutableMap.of("dimensions", ImmutableList.of("dim1", "dim2", "dim3")),
        "metrics", ImmutableList.of(
            ImmutableMap.of("type", "min", "name", "min", "fieldName", "min", "inputType", "double"),
            ImmutableMap.of("type", "max", "name", "max", "fieldName", "max", "inputType", "double")
        )
    );
    IncrementalIndexSchema schema = new DefaultObjectMapper().convertValue(ingestion, IncrementalIndexSchema.class);
    Assert.assertEquals(Arrays.asList("dim1", "dim2", "dim3"), schema.getDimensionsSpec().getDimensionNames());
    AggregatorFactory[] factories = schema.getMetrics();
    Assert.assertEquals(2, factories.length);
    Assert.assertEquals(new GenericMinAggregatorFactory("min", "min", ValueDesc.DOUBLE), factories[0]);
    Assert.assertEquals(new GenericMaxAggregatorFactory("max", "max", ValueDesc.DOUBLE), factories[1]);
  }
}
