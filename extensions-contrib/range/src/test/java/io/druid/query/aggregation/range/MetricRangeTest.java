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

    float[] data = {0.1f, 0.8f, 0.3f, 0.6f, 0.5f, 0.2f, 0.1f, 0.7f};

    for (float val : data)
    {
      mr.add(val);
    }

    MetricRange expected = new MetricRange(0.1f, 0.8f);

    Assert.assertEquals(expected, mr);
    Assert.assertEquals(0.7f, mr.getRange(), 0.001);
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
