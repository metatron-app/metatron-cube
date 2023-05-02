/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.List;

public class DelegatedAggregatorFactory extends AggregatorFactory
{
  protected final AggregatorFactory delegate;

  public DelegatedAggregatorFactory(AggregatorFactory delegate)
  {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    return delegate.factorize(columnSelectorFactory);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return delegate.factorizeBuffered(columnSelectorFactory);
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public BinaryFn.Identical combiner()
  {
    return delegate.combiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  protected boolean isMergeable(AggregatorFactory other)
  {
    return delegate.isMergeable(other);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (!isMergeable(other)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    return delegate.getMergingFactory(other);
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return delegate.finalizeComputation(object);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return delegate.finalizedType();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return delegate.getCacheKey(builder);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return delegate.getOutputType();
  }

  @Override
  public ValueDesc getInputType()
  {
    return delegate.getInputType();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize();
  }

  @JsonProperty
  public AggregatorFactory unwrap()
  {
    return delegate;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return delegate.equals(((DelegatedAggregatorFactory) o).delegate);
  }

  @Override
  public int hashCode()
  {
    return delegate.hashCode();
  }

  @Override
  public String toString()
  {
    return "DelegatedAggregatorFactory{" +
           "delegate=" + delegate +
           '}';
  }
}
