package io.druid.query.aggregation.median;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.TestFloatColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class DruidTDigestAggregatorTest {

  private void aggregate(TestFloatColumnSelector selector, Aggregator aggregator)
  {
    aggregator.aggregate();
    selector.increment();
  }

  private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator aggregator, ByteBuffer buf, int position)
  {
    aggregator.aggregate(buf, position);
    selector.increment();
  }

  @Test
  public void testDruidTDigest()
  {
    final float[] values = {1, 1000, 1000, 1000};

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

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
    final double[] expected = Doubles.toArray(Floats.asList(values));

    Assert.assertArrayEquals("quartile values do not match", expected, quartiles, 0);
  }

  @Test
  public void testBufferAggregate() throws Exception
  {
    final float[] values = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
    final int compression = 10;

    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

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
}
