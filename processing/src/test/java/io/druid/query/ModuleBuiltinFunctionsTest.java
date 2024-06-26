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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

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

  @Test
  public void testMurmurHash3()
  {
    Assert.assertEquals(104593805, Evals.eval(Parser.parse("hash('navis')"), null).intValue());
    Assert.assertEquals(13, Evals.eval(Parser.parse("hash('navis') % 32"), null).intValue());

    Assert.assertEquals(994288410, Evals.eval(Parser.parse("murmur32('navis')"), null).intValue());
    Assert.assertEquals(26, Evals.eval(Parser.parse("murmur32('navis') % 32"), null).intValue());

    Assert.assertEquals(1058442040386139741L, Evals.eval(Parser.parse("murmur64('navis')"), null).longValue());
    Assert.assertEquals(29, Evals.eval(Parser.parse("murmur64('navis') % 32"), null).longValue());
  }

  @Test
  public void testToJson() throws Exception
  {
    String json = "{\"context\":{\"key1\": \"value1\", \"key2\":\"value2\"}}";
    Map<String, Object> map = TestHelper.JSON_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});

    Assert.assertEquals(
        "{\"key1\":\"value1\",\"key2\":\"value2\"}",
        Evals.evalValue(Parser.parse("toJson(context)"), Parser.withMap(map))
    );
  }
}
