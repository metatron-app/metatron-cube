package io.druid.query.aggregation.range;

import org.junit.Assert;
import org.junit.Test;

public class MetricRangeTest
{
  public MetricRangeTest()
  {

  }

  @Test
  public void testSimple()
  {
    MetricRange mr = new MetricRange();

    double[] data = {0.1, 0.8, 0.3, 0.6, 0.5, 0.2, 0.1, 0.7};

    for (double val : data)
    {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(0.1, 0.8);

    Assert.assertEquals(expected, mr);
    Assert.assertEquals(0.7, mr.getRange(), 0.001);
  }

  @Test
  public void testAdd()
  {
    MetricRange mr1 = new MetricRange(0, 10);
    MetricRange mr2 = new MetricRange(20, 30);

    MetricRange mr3 = new MetricRange(0, 30);

    Assert.assertEquals(mr3, mr1.add(mr2));
  }
}
