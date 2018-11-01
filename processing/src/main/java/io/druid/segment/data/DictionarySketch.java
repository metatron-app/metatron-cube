package io.druid.segment.data;

import com.metamx.common.IAE;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import io.druid.data.ValueDesc;
import io.druid.query.sketch.SketchOp;
import io.druid.query.sketch.TypedSketch;

import java.nio.ByteBuffer;

public class DictionarySketch
{
  public static DictionarySketch of(ByteBuffer buffer)
  {
    final byte version = buffer.get();
    if (version != SketchWriter.version) {
      throw new IAE("Unknown version[%s]", version);
    }
    byte flag = buffer.get();
    ByteBuffer quantile = ByteBufferSerializer.prepareForRead(buffer);
    ByteBuffer theta = ByteBufferSerializer.prepareForRead(buffer);
    return new DictionarySketch(flag, quantile, theta);
  }

  private final byte flag;
  private final ByteBuffer quantile;
  private final ByteBuffer theta;

  public DictionarySketch(byte flag, ByteBuffer quantile, ByteBuffer theta)
  {
    this.flag = flag;
    this.quantile = quantile;
    this.theta = theta;
  }

  public byte getFlag()
  {
    return flag;
  }

  public ItemsSketch getQuantile(ValueDesc valueDesc)
  {
    return (ItemsSketch) TypedSketch.readPart(quantile, SketchOp.QUANTILE, valueDesc);
  }

  public Sketch getTheta(ValueDesc valueDesc)
  {
    return (Sketch) TypedSketch.readPart(theta, SketchOp.THETA, valueDesc);
  }
}
