package io.druid.segment.data;

import java.io.Closeable;

/**
 */
public interface IndexedDoubles extends Closeable
{
  public int size();
  public double get(int index);
  public void fill(int index, double[] toFill);
}
