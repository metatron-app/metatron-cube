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

package io.druid.hive;

import com.google.common.collect.Maps;
import io.druid.common.guava.DSuppliers;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HiveFunctionsTest
{
  static {
    Parser.register(HiveFunctions.class);
  }

  @Test
  public void test()
  {
    Map<String, DSuppliers.TypedSupplier> mapping = Maps.newHashMap();
    mapping.put("x", new DSuppliers.TypedSupplier.Simple<String>("   xxx  ", ValueDesc.STRING));
    Expr expr = Parser.parse("hive.trim(x)", true);
    ExprEval eval = expr.eval(Parser.withTypedSuppliers(mapping));
    Assert.assertEquals("xxx", eval.value());
  }
}