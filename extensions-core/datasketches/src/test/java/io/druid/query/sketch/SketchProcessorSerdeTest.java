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

package io.druid.query.sketch;

import io.druid.query.PostProcessingOperator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SketchProcessorSerdeTest
{
  @Test
  public void testTheta() throws IOException
  {
    String value = SketchQueryRunnerTest.JSON_MAPPER.writeValueAsString(new SketchThetaProcessor());
    Assert.assertEquals("{\"type\":\"sketch.theta\"}", value);
    PostProcessingOperator operator = SketchQueryRunnerTest.JSON_MAPPER.readValue(value, PostProcessingOperator.class);
    Assert.assertTrue(operator instanceof SketchThetaProcessor);
  }

  @Test
  public void testSampling() throws IOException
  {
    String value = SketchQueryRunnerTest.JSON_MAPPER.writeValueAsString(new SketchSamplingProcessor());
    Assert.assertEquals("{\"type\":\"sketch.sampling\"}", value);
    PostProcessingOperator operator = SketchQueryRunnerTest.JSON_MAPPER.readValue(value, PostProcessingOperator.class);
    Assert.assertTrue(operator instanceof SketchSamplingProcessor);
  }

  @Test
  public void testFrequency() throws IOException
  {
    String value = SketchQueryRunnerTest.JSON_MAPPER.writeValueAsString(new SketchFrequencyProcessor());
    Assert.assertEquals("{\"type\":\"sketch.frequency\"}", value);
    PostProcessingOperator operator = SketchQueryRunnerTest.JSON_MAPPER.readValue(value, PostProcessingOperator.class);
    Assert.assertTrue(operator instanceof SketchFrequencyProcessor);
  }
}
