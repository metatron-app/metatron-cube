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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.common.KeyBuilder;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.ColumnSelectorFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class FilteredAggregatorFactory extends DelegatedAggregatorFactory
{
  public static FilteredAggregatorFactory of(AggregatorFactory factory, DimFilter predicate)
  {
    if (factory instanceof FilteredAggregatorFactory) {
      FilteredAggregatorFactory inner = (FilteredAggregatorFactory) factory;
      return new FilteredAggregatorFactory(inner.unwrap(), DimFilters.and(inner.getFilter(), predicate));
    }
    return new FilteredAggregatorFactory(factory, predicate);
  }

  private static final byte CACHE_TYPE_ID = 0x9;
  private final DimFilter filter;

  public FilteredAggregatorFactory(
      @JsonProperty("aggregator") AggregatorFactory delegate,
      @JsonProperty("filter") DimFilter filter
  )
  {
    super(delegate);
    this.filter = Preconditions.checkNotNull(filter);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory)
  {
    return Aggregators.wrap(
        columnSelectorFactory.makePredicateMatcher(filter), delegate.factorize(columnSelectorFactory)
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory)
  {
    return Aggregators.wrap(
        columnSelectorFactory.makePredicateMatcher(filter), delegate.factorizeBuffered(columnSelectorFactory)
    );
  }

  @Override
  protected boolean isMergeable(AggregatorFactory other)
  {
    return super.isMergeable(other) && Objects.equals(filter, ((FilteredAggregatorFactory) other).filter);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (!isMergeable(other)) {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
    return new FilteredAggregatorFactory(
        delegate.getMergingFactory(((FilteredAggregatorFactory) other).delegate), filter
    );
  }

  @Override
  public List<String> requiredFields()
  {
    Set<String> required = Sets.newLinkedHashSet(delegate.requiredFields());
    filter.addDependent(required);
    return Lists.newArrayList(required);
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(filter)
                  .append(delegate);
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
           "delegate=" + delegate +
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
