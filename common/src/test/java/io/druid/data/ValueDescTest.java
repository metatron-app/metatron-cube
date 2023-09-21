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

package io.druid.data;

import org.junit.Assert;
import org.junit.Test;

import static io.druid.data.ValueDesc.DIMENSION_TYPE;
import static io.druid.data.ValueDesc.STRING_DIMENSION_TYPE;
import static io.druid.data.ValueDesc.STRING_MV_DIMENSION_TYPE;
import static io.druid.data.ValueDesc.STRING_MV_TYPE;
import static io.druid.data.ValueDesc.STRING_TYPE;

public class ValueDescTest
{
  @Test
  public void testMergeDimensions()
  {
    // DIMENSION_TYPE, STRING_DIMENSION_TYPE, STRING_MV_DIMENSION_TYPE
    Assert.assertEquals(DIMENSION_TYPE, ValueDesc.merge(DIMENSION_TYPE, DIMENSION_TYPE));
    Assert.assertEquals(STRING_DIMENSION_TYPE, ValueDesc.merge(STRING_DIMENSION_TYPE, STRING_DIMENSION_TYPE));
    Assert.assertEquals(STRING_MV_DIMENSION_TYPE, ValueDesc.merge(STRING_MV_DIMENSION_TYPE, STRING_MV_DIMENSION_TYPE));

    Assert.assertEquals(STRING_DIMENSION_TYPE, ValueDesc.merge(STRING_DIMENSION_TYPE, DIMENSION_TYPE));
    Assert.assertEquals(STRING_DIMENSION_TYPE, ValueDesc.merge(DIMENSION_TYPE, STRING_DIMENSION_TYPE));

    Assert.assertEquals(STRING_MV_DIMENSION_TYPE, ValueDesc.merge(STRING_MV_DIMENSION_TYPE, DIMENSION_TYPE));
    Assert.assertEquals(STRING_MV_DIMENSION_TYPE, ValueDesc.merge(DIMENSION_TYPE, STRING_MV_DIMENSION_TYPE));

    Assert.assertEquals(STRING_MV_DIMENSION_TYPE, ValueDesc.merge(STRING_MV_DIMENSION_TYPE, STRING_DIMENSION_TYPE));
    Assert.assertEquals(STRING_MV_DIMENSION_TYPE, ValueDesc.merge(STRING_DIMENSION_TYPE, STRING_MV_DIMENSION_TYPE));

    Assert.assertNull(ValueDesc.merge(DIMENSION_TYPE, STRING_MV_TYPE));
    Assert.assertNull(ValueDesc.merge(STRING_DIMENSION_TYPE, STRING_MV_TYPE));
    Assert.assertNull(ValueDesc.merge(STRING_MV_DIMENSION_TYPE, STRING_MV_TYPE));

    Assert.assertNull(ValueDesc.merge(DIMENSION_TYPE, STRING_TYPE));
    Assert.assertNull(ValueDesc.merge(STRING_DIMENSION_TYPE, STRING_TYPE));
    Assert.assertNull(ValueDesc.merge(STRING_MV_DIMENSION_TYPE, STRING_TYPE));
  }
}