package io.druid.query.aggregation.median;

import com.google.common.primitives.Doubles;
import io.druid.query.aggregation.*;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DruidTDigestAggregatorTest {

  private void aggregate(TestObjectColumnSelector selector, Aggregator aggregator)
  {
    aggregator.aggregate();
    selector.increment();
  }

  private void aggregateBuffer(TestObjectColumnSelector selector, BufferAggregator aggregator, ByteBuffer buf, int position)
  {
    aggregator.aggregate(buf, position);
    selector.increment();
  }

  @Test
  public void testDruidTDigest()
  {
    final Double[] values = {1d, 1000d, 1000d, 1000d};

    final TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

    DruidTDigestAggregator aggregator = new DruidTDigestAggregator("test", selector, 10);

    for (double value: values) {
      aggregate(selector, aggregator);
    }

    DruidTDigest digest = (DruidTDigest) aggregator.get();
    double median = digest.median();

    Assert.assertEquals("median value does not match", 1000.0, median, 0);

    double quartile = digest.quantile(0.25);

    Assert.assertEquals("quartile value does not match", 1.0, quartile, 0);

    double[] quartiles = digest.quantiles(new double[] {0.25, 0.5, 0.75, 1});
    final double[] expected = ArrayUtils.toPrimitive(values);

    Assert.assertArrayEquals("quartile values do not match", expected, quartiles, 0);
  }

  @Test
  public void testBufferAggregate() throws Exception
  {
    final Double[] values = {23d, 19d, 10d, 16d, 36d, 2d, 9d, 32d, 30d, 45d};
    final int compression = 10;

    final TestObjectColumnSelector selector = new TestObjectColumnSelector((Object[])values);

    DruidTDigestAggregatorFactory factory = new DruidTDigestAggregatorFactory(
        "billy", "billy", compression
    );
    DruidTDigestBufferAggregator agg = new DruidTDigestBufferAggregator(selector, compression);

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    for (int i = 0; i < values.length; i++) {
      aggregateBuffer(selector, agg, buf, position);
    }

    DruidTDigest digest = ((DruidTDigest) agg.get(buf, position));

    Assert.assertEquals(
        "median don't match expected value", 21.0, digest.median(), 0
    );

    Assert.assertEquals(
        "quantile(0.1) don't match expected value", 2.0, digest.quantile(0.1), 0
    );

    Assert.assertEquals(
        "quantile(0.7) don't match expected value", 30.0, digest.quantile(0.7), 0
    );

    Assert.assertEquals(
        "quantile(0.75) don't match expected value", 32.0, digest.quantile(0.75), 0
    );

    Assert.assertArrayEquals("quantiles({0.1, 0.7, 0.75} don't match expected value",
        new double[] {2.0, 30.0, 32.0}, digest.quantiles(new double[]{0.1, 0.7, 0.75}), 0);
  }

  @Test
  public void testDoubleValues()
  {
    final Double[] values = {1.4633397148E9,
        1.4633397198E9,
        1.4633397248E9,
        1.4633397298E9,
        1.4633397168E9,
        1.4633397218E9,
        1.4633397268E9,
        1.4633397318E9,
        1.4633397178E9,
        1.4633397228E9,
        1.4633397278E9,
        1.4633397328E9,
        1.4633397158E9,
        1.4633397208E9,
        1.4633397258E9,
        1.4633397308E9,
        1.4633397138E9,
        1.4633397188E9,
        1.4633397238E9,
        1.4633397288E9
    };

    final TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

    DruidTDigestAggregator aggregator = new DruidTDigestAggregator("test", selector, 10);

    for (double value: values) {
      aggregate(selector, aggregator);
    }

    Arrays.sort(values);
    DruidTDigest digest = (DruidTDigest) aggregator.get();
    double median = digest.median();

    double expected = (values.length % 2 == 0) ? (values[values.length / 2] + values[values.length / 2 - 1]) / 2 : values[values.length / 2];
    Assert.assertEquals("median value does not match", expected , median, 0);

    double quartile = digest.quantile(0.25);

    Assert.assertEquals("quartile value does not match", 1.4633397178E9, quartile, 0);

    double[] quartiles = digest.quantiles(new double[] {0.25, 0.5, 0.75, 1});
    final double[] expectedQuantile = {1.4633397178E9, 1.4633397228E9, 1.4633397278E9, 1.4633397328E9};

    Assert.assertArrayEquals("quartile values do not match", expectedQuantile, quartiles, 0);
  }
}
