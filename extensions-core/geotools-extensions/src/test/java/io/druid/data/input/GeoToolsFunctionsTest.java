package io.druid.data.input;

import io.druid.data.GeoToolsFunctions;
import io.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Test;

public class GeoToolsFunctionsTest
{
  static {
    Parser.register(GeoToolsFunctions.class);
  }

  @Test
  public void test()
  {
    double[] converted = (double[])
        Parser.parse("lonlat.to4326('EPSG:3857', -8575605.398444, 4707174.018280)").eval(null).value();
    Assert.assertArrayEquals(new double[]{-77.03597400000125, 38.89871699999715}, converted, 0.00001);

    converted = (double[])
        Parser.parse("lonlat.to4326('EPSG:4301', -77.03597400000125, 38.89871699999715)").eval(null).value();
    Assert.assertArrayEquals(new double[]{-77.03631683718933, 38.907094818962875}, converted, 0.00001);
  }
}
