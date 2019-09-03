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

package io.druid.query.aggregation.post;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.PostAggregator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class MathPostAggregatorTest
{
  @Test
  public void test() throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    String x =
        "[{\"type\":\"math\",\"name\":\"HDV누적_전국_평균\",\"expression\":\"(aggregationfunc_000/count)\"},"
        + "{\"type\":\"math\",\"name\":\"CSFB누적_전국_평균\",\"expression\":\"(aggregationfunc_002/count)\"},"
        + "{\"type\":\"math\",\"name\":\"D(3rd제외)누적_전국_평균\",\"expression\":\"(aggregationfunc_004/count)\"}," 
        + "{\"type\":\"math\",\"name\":\"D(3rd포함)누적_전국_평균\",\"expression\":\"aggregationfunc_006/aggregationfunc_007\"},"
        + "{\"type\":\"math\",\"name\":\"H2.0누적_전국_평균\",\"expression\":\"aggregationfunc_008/aggregationfunc_009\"},"
        + "{\"type\":\"math\",\"name\":\"D2.0누적_전국_평균\",\"expression\":\"case(aggregationfunc_010/aggregationfunc_011=='NaN','',aggregationfunc_012/aggregationfunc_013=='Infinity','',aggregationfunc_014/aggregationfunc_015=='-Infinity','',aggregationfunc_016/aggregationfunc_017)\"}]";
    PostAggregator[] actual = mapper.readValue(x, PostAggregator[].class);
    System.out.println(Arrays.toString(actual));
  }
}