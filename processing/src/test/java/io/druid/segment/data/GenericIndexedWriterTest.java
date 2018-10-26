package io.druid.segment.data;

import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class GenericIndexedWriterTest
{
  @Test
  public void test() throws IOException
  {
    IOPeon ioPeon = new TmpFileIOPeon();
    ObjectStrategy<String> strategy = ObjectStrategy.STRING_STRATEGY;
    GenericIndexedWriter<String> writer = new GenericIndexedWriter<String>(ioPeon, "test", strategy, true);
    writer.open();
    writer.add("a");
    writer.add("b");
    writer.add("c");
    writer.close();

    ItemsSketch quantile1 = writer.getQuantile();
    Sketch theta1 = writer.getTheta();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (WritableByteChannel channel = Channels.newChannel(out)) {
      writer.writeToChannel(channel);
    }
    Assert.assertEquals(writer.getSerializedSize(), out.size());

    GenericIndexed<String> indexed = GenericIndexed.read(ByteBuffer.wrap(out.toByteArray()), strategy);
    Assert.assertEquals(3, indexed.size());
    Assert.assertEquals(0, indexed.indexOf("a"));
    Assert.assertEquals(1, indexed.indexOf("b"));
    Assert.assertEquals(2, indexed.indexOf("c"));
    Assert.assertTrue(indexed.indexOf("d") < 0);

    ItemsSketch quantile2 = indexed.getQuantile();
    Assert.assertEquals(quantile1.toString(true, true), quantile2.toString(true, true));

    // head vs direct.. skip summary part
    Sketch theta2 = indexed.getTheta();
    Assert.assertEquals(theta1.toString(false, true, 8, true), theta2.toString(false, true, 8, true));
  }
}
