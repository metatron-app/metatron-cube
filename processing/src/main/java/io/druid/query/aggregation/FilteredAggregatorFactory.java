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
import io.druid.query.filter.DimFilter;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class FilteredAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x9;

  private final AggregatorFactory delegate;
  private final DimFilter filter;

  public FilteredAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("filter") DimFilter filter
  )
  {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(filter);

    this.delegate = delegate;
    this.filter = filter;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    return new FilteredAggregator(
        columnSelectorFactory.makePredicateMatcher(filter),
        delegate.factorize(columnSelectorFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return new FilteredBufferAggregator(
        columnSelectorFactory.makePredicateMatcher(filter),
        delegate.factorizeBuffered(columnSelectorFactory)
    );
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return delegate.combiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    FilteredAggregatorFactory filtered = checkMergeable(other);
    if (!Objects.equals(filter, filtered.filter)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    return new FilteredAggregatorFactory(delegate.getMergingFactory(filtered.delegate), filter);
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
    return builder.append(CACHE_TYPE_ID)
                  .append(filter)
                  .append(delegate);
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
  public AggregatorFactory getAggregator()
  {
    return delegate;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public String toString()
  {
    return "FilteredAggregatorFactory{" +
           ", delegate=" + delegate +
           ", filter=" + filter +
           '}';
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

    FilteredAggregatorFactory that = (FilteredAggregatorFactory) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }
    if (!filter.equals(that.filter)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = delegate.hashCode();
    result = 31 * result + filter.hashCode();
    return result;
  }
}
