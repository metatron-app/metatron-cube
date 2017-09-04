package io.druid.segment.data;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class FloatRanges
{
  public static ObjectStrategy<FloatRanges> getStrategy(final BitmapSerdeFactory serdeFactory)
  {
    final ObjectStrategy<ImmutableBitmap> strategy = serdeFactory.getObjectStrategy();
    return new ObjectStrategy.NotComparable<FloatRanges>()
    {
      @Override
      public Class<? extends FloatRanges> getClazz()
      {
        return FloatRanges.class;
      }

      @Override
      public FloatRanges fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        float[] breaks = new float[buffer.getInt()];
        for (int i = 0; i < breaks.length; i++) {
          breaks[i] = buffer.getFloat();
        }
        ImmutableBitmap[] bitmaps = new ImmutableBitmap[breaks.length - 1];
        for (int i = 0; i < bitmaps.length; i++) {
          bitmaps[i] = ByteBufferSerializer.read(buffer, strategy);
        }
        return new FloatRanges(serdeFactory.getBitmapFactory(), breaks, bitmaps);
      }

      @Override
      public byte[] toBytes(FloatRanges val)
      {
        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        bout.writeInt(val.breaks.length);
        for (float aBreak : val.breaks) {
          bout.writeFloat(aBreak);
        }
        for (ImmutableBitmap bin : val.bins) {
          byte[] bytes = strategy.toBytes(bin);
          bout.writeInt(bytes.length);
          bout.write(bytes);
        }
        return bout.toByteArray();
      }

      @Override
      public int compare(FloatRanges o1, FloatRanges o2)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private final BitmapFactory factory;
  private final float[] breaks;   // in-ex in-ex... in-in (includes max)
  private final ImmutableBitmap[] bins;

  public FloatRanges(BitmapFactory factory, float[] breaks, ImmutableBitmap[] bins)
  {
    this.breaks = breaks;
    this.bins = bins;
    this.factory = factory;
  }

  public ImmutableBitmap of(Range<Float> range)
  {
    if (range.isEmpty()) {
      return factory.makeEmptyImmutableBitmap();
    }
    int from = 0;
    int to = bins.length - 1;
    if (range.hasLowerBound()) {
      float lower = range.lowerEndpoint();
      int index = Arrays.binarySearch(breaks, lower);
      if (index == bins.length) {
        return range.lowerBoundType() == BoundType.CLOSED ? bins[index - 1] : factory.makeEmptyImmutableBitmap();
      }
      from = index < 0 ? -index - 2 : index;
    }
    if (range.hasUpperBound()) {
      float upper = range.upperEndpoint();
      int index = Arrays.binarySearch(breaks, upper);
      if (index == -1 || (index == 0 && range.upperBoundType() == BoundType.OPEN)) {
        return factory.makeEmptyImmutableBitmap();
      }
      to = Math.min(to, index < 0 ? -index - 2 : range.upperBoundType() == BoundType.OPEN ? index - 1 : index);
    }
    if (from == to) {
      return bins[from];
    }
    return factory.union(Arrays.asList(bins).subList(from, to + 1));
  }

  public float[] getBreaks()
  {
    return breaks;
  }

  public ImmutableBitmap[] getBins()
  {
    return bins;
  }

  public int[] getSizes()
  {
    int[] sizes = new int[bins.length];
    for (int i = 0; i < sizes.length; i++) {
      sizes[i] = bins[i].size();
    }
    return sizes;
  }

  public float getMin()
  {
    return breaks[0];
  }

  public float getMax()
  {
    return breaks[bins.length];
  }

  @Override
  public String toString()
  {
    return Arrays.toString(breaks) + ":" + Arrays.toString(getSizes());
  }
}
