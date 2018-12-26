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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.druid.common.guava.DSuppliers;
import io.druid.data.input.Row;
import io.druid.query.RowResolver;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.ColumnSelectorFactories;

import java.util.Objects;

@Deprecated
public class DimFilterHavingSpec implements HavingSpec
{
  private final DimFilter dimFilter;

  @JsonCreator
  public DimFilterHavingSpec(
      @JsonProperty("filter") final DimFilter dimFilter
  )
  {
    this.dimFilter = Preconditions.checkNotNull(dimFilter, "filter");
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Override
  public Predicate<Row> toEvaluator(RowResolver resolver)
  {
    final DSuppliers.HandOver<Row> supplier = new DSuppliers.HandOver<>();
    final ValueMatcher matcher =
        dimFilter.toFilter().makeMatcher(
            new ColumnSelectorFactories.FromRowSupplier(supplier, resolver)
        );
    return new Predicate<Row>()
    {
      @Override
      public boolean apply(Row input)
      {
        supplier.set(input);
        return matcher.matches();
      }
    };
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DimFilterHavingSpec that = (DimFilterHavingSpec) o;
    return Objects.equals(dimFilter, that.dimFilter);
  }

  @Override
  public int hashCode()
  {
    return dimFilter.hashCode();
  }

  @Override
  public String toString()
  {
    return "DimFilterHavingSpec{" +
           "dimFilter=" + dimFilter +
           '}';
  }
}
