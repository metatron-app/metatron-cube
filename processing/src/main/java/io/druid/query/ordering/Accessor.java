package io.druid.query.ordering;

import com.google.common.primitives.Longs;

import java.util.Comparator;

public interface Accessor<T>
{
  Object get(T source);

  class TimeComparator<T> implements Comparator<T>
  {
    private final Accessor<T> accessor;

    public TimeComparator(Accessor<T> accessor) {this.accessor = accessor;}

    @Override
    public int compare(T left, T right)
    {
      return Longs.compare((Long) accessor.get(left), (Long) accessor.get(right));
    }
  }

  class ComparatorOn<T> implements Comparator<T>
  {
    private final Comparator comparator;
    private final Accessor<T> accessor;

    public ComparatorOn(Comparator comparator, Accessor<T> accessor)
    {
      this.comparator = comparator;
      this.accessor = accessor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(T left, T right)
    {
      return comparator.compare(accessor.get(left), accessor.get(right));
    }
  }
}

