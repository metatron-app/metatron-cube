package io.druid.query.ordering;

public interface Accessor<T>
{
  Object get(T source);
}

