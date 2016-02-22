/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public abstract class VarianceAggregator implements Aggregator
{
  protected final String name;

  protected final VarianceHolder holder = new VarianceHolder();

  public VarianceAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void reset()
  {
    holder.reset();
  }

  @Override
  public Object get()
  {
    return holder;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("VarianceAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("VarianceAggregator does not support getLong()");
  }

  public static final class FloatInput extends VarianceAggregator
  {
    private final FloatColumnSelector selector;

    public FloatInput(String name, FloatColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      holder.add(selector.get());
    }

    @Override
    public Aggregator clone()
    {
      return new FloatInput(name, selector);
    }
  }

  public static final class LongInput extends VarianceAggregator
  {
    private final LongColumnSelector selector;

    public LongInput(String name, LongColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      holder.add(selector.get());
    }

    @Override
    public Aggregator clone()
    {
      return new LongInput(name, selector);
    }
  }

  public static final class ObjectInput extends VarianceAggregator
  {
    private final ObjectColumnSelector selector;

    public ObjectInput(String name, ObjectColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      VarianceHolder.combineValues(holder, selector.get());
    }

    @Override
    public Aggregator clone()
    {
      return new ObjectInput(name, selector);
    }
  }
}
