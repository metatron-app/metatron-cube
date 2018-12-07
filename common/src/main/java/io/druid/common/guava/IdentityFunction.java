package io.druid.common.guava;

import com.google.common.base.Function;

public interface IdentityFunction<T> extends Function<T, T>
{
  IdentityFunction INSTANCE = new IdentityFunction()
  {
    @Override
    public Object apply(Object input)
    {
      return input;
    }
  };
}
