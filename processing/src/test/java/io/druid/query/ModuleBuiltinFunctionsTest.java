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