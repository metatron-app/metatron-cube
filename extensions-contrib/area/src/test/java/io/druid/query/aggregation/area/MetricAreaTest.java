package io.druid.query.aggregation.area;

import org.junit.Assert;
import org.junit.Test;

public class MetricAreaTest
{
  public MetricAreaTest()
  {

  }

  @Test
  public void testSimple()
  {
    MetricArea metricArea = new MetricArea();

    double[] data = {0.1, 0.8, 0.3, 0.6, 0.5, 0.2, 0.1, 0.7};

    for (double val : data)
    {
      metricArea.add(val);
    }

    Assert.assertEquals(3.3, metricArea.sum, 0.001);
    Assert.assertEquals(8, metricArea.count);
    Assert.assertEquals(0.1, metricArea.min, 0.001);
    Assert.assertEquals(2.5, metricArea.getArea(), 0.001);
  }

  @Test
  public void testAdd()
  {
    MetricArea ma1 = new MetricArea(20, 10, 1);
    MetricArea ma2 = new MetricArea(30, 15, 2);

    MetricArea ma3 = new MetricArea(50, 25, 1);

    Assert.assertEquals(ma3, ma1.add(ma2));
  }
}
