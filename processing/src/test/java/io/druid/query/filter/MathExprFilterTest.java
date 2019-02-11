package io.druid.query.filter;

import org.junit.Assert;
import org.junit.Test;

public class MathExprFilterTest
{
  @Test
  public void testOptimize()
  {
    Assert.assertTrue(new MathExprFilter("'L_ORDERKEY'=='L_ORDERKEY'").optimize() instanceof DimFilters.ALL);
    Assert.assertTrue(new MathExprFilter("'L_ORDERKEY'!='L_ORDERKEY'").optimize() instanceof DimFilters.NONE);
  }
}
