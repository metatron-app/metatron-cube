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

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class ModuleBuiltinFunctionsTest
{
  static {
    Parser.register(ModuleBuiltinFunctions.class);
    ModuleBuiltinFunctions.jsonMapper = TestHelper.JSON_MAPPER;
  }

  @Test
  public void testLookupMap()
  {
    Expr expr = Parser.parse("lookupMap('{\"key\": \"value\"}', x, retainMissingValue='true')");
    Assert.assertEquals("value", expr.eval(Parser.withMap(ImmutableMap.of("x", "key"))).value());
    Assert.assertEquals("key2", expr.eval(Parser.withMap(ImmutableMap.of("x", "key2"))).value());
  }
}