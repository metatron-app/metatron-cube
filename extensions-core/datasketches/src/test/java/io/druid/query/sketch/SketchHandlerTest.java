package io.druid.query.sketch;

import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.quantiles.ItemsUnion;
import io.druid.data.ValueType;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class SketchHandlerTest
{
  @Test
  public void testQuantile() {
    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch = q.newUnion(16, ValueType.FLOAT, null);

    q.updateWithValue(sketch, 1.5f);
    q.updateWithValue(sketch, 2.5f);
    q.updateWithValue(sketch, 3.5f);
    q.updateWithValue(sketch, 4.5f);
    q.updateWithValue(sketch, 2.5f);
    q.updateWithValue(sketch, 1.5f);
    q.updateWithValue(sketch, -1.5f);
    q.updateWithValue(sketch, -3.5f);
    q.updateWithValue(sketch, 4.5f);
    q.updateWithValue(sketch, 7.5f);
    q.updateWithValue(sketch, 11.5f);

    q.updateWithValue(sketch, 1.2f);
    q.updateWithValue(sketch, 2.2f);
    q.updateWithValue(sketch, 3.2f);
    q.updateWithValue(sketch, 4.2f);
    q.updateWithValue(sketch, 2.2f);
    q.updateWithValue(sketch, 1.2f);
    q.updateWithValue(sketch, -1.2f);
    q.updateWithValue(sketch, -3.2f);
    q.updateWithValue(sketch, 4.2f);
    q.updateWithValue(sketch, 7.2f);
    q.updateWithValue(sketch, 11.2f);

    ItemsSketch<Float> r = q.toSketch(sketch).value();
    Assert.assertArrayEquals(new Float[] {-3.5f, -1.2f, 1.5f, 2.5f, 4.2f, 7.2f, 11.5f}, r.getQuantiles(7));
  }

  @Test
  public void testQuantileMerge() {
    SketchHandler.Quantile q = new SketchHandler.Quantile();
    TypedSketch<ItemsUnion> sketch1 = q.newUnion(16, ValueType.FLOAT, null);

    q.updateWithValue(sketch1, 1.5f);
    q.updateWithValue(sketch1, 2.5f);
    q.updateWithValue(sketch1, 3.5f);
    q.updateWithValue(sketch1, 4.5f);
    q.updateWithValue(sketch1, 2.5f);
    q.updateWithValue(sketch1, 1.5f);
    q.updateWithValue(sketch1, -1.5f);
    q.updateWithValue(sketch1, -3.5f);
    q.updateWithValue(sketch1, 4.5f);
    q.updateWithValue(sketch1, 7.5f);
    q.updateWithValue(sketch1, 11.5f);

    TypedSketch<ItemsUnion> sketch2 = q.newUnion(16, ValueType.FLOAT, null);
    q.updateWithValue(sketch2, 1.2f);
    q.updateWithValue(sketch2, 2.2f);
    q.updateWithValue(sketch2, 3.2f);
    q.updateWithValue(sketch2, 4.2f);
    q.updateWithValue(sketch2, 2.2f);
    q.updateWithValue(sketch2, 1.2f);
    q.updateWithValue(sketch2, -1.2f);
    q.updateWithValue(sketch2, -3.2f);
    q.updateWithValue(sketch2, 4.2f);
    q.updateWithValue(sketch2, 7.2f);
    q.updateWithValue(sketch2, 11.2f);

    SketchBinaryFn binary = new SketchBinaryFn(16, q);
    TypedSketch<ItemsSketch> sketch = binary.merge(q.toSketch(sketch1), q.toSketch(sketch2));
    ItemsSketch<Float> r = sketch.value();
    Assert.assertArrayEquals(new Float[] {-3.5f, -1.2f, 1.5f, 2.5f, 4.2f, 7.2f, 11.5f}, r.getQuantiles(7));
  }
}
