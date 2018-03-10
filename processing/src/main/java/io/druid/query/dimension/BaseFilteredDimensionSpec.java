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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListBasedIndexedInts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public abstract class BaseFilteredDimensionSpec extends DimensionSpec.Abstract
{
  protected final DimensionSpec delegate;

  public BaseFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate
  )
  {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null");
  }

  @JsonProperty
  public DimensionSpec getDelegate()
  {
    return delegate;
  }

  @Override
  public String getDimension()
  {
    return delegate.getDimension();
  }

  @Override
  public String getOutputName()
  {
    return delegate.getOutputName();
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    return delegate.getExtractionFn();
  }

  @Override
  public boolean preservesOrdering()
  {
    return delegate.preservesOrdering();
  }

  @Override
  public DimensionSelector decorate(DimensionSelector selector)
  {
    return selector;
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

    BaseFilteredDimensionSpec that = (BaseFilteredDimensionSpec) o;

    if (!delegate.equals(that.delegate)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return delegate.hashCode();
  }

  protected static DimensionSelector decorate(
      final DimensionSelector selector,
      final Map<Integer, Integer> forwardMapping,
      final int[] reverseMapping
  )
  {
    if (selector == null) {
      return selector;
    }

    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        IndexedInts baseRow = selector.getRow();
        List<Integer> result = new ArrayList<>(baseRow.size());

        for (int i : baseRow) {
          if (forwardMapping.containsKey(i)) {
            result.add(forwardMapping.get(i));
          }
        }

        return new ListBasedIndexedInts(result);
      }

      @Override
      public int getValueCardinality()
      {
        return forwardMapping.size();
      }

      @Override
      public Comparable lookupName(int id)
      {
        return selector.lookupName(reverseMapping[id]);
      }

      @Override
      public Class type()
      {
        return selector.type();
      }

      @Override
      public int lookupId(Comparable name)
      {
        return forwardMapping.get(selector.lookupId(name));
      }
    };
  }
}
