/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.aggregation;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.segment.VirtualColumn;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Aggregation
{
  private final List<VirtualColumn> virtualColumns;
  private final List<AggregatorFactory> aggregatorFactories;
  private final PostAggregator postAggregator;
  private final String outputName;
  private final ValueDesc outputType;

  private Aggregation(
      final RowSignature sourceRowSignature,
      final List<VirtualColumn> virtualColumns,
      final List<AggregatorFactory> aggregatorFactories,
      final PostAggregator postAggregator
  )
  {
    final Supplier<TypeResolver> resolver = Suppliers.ofInstance(sourceRowSignature);
    this.virtualColumns = Preconditions.checkNotNull(virtualColumns, "virtualColumns");
    this.aggregatorFactories = ImmutableList.copyOf(Iterables.transform(
            Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories"),
            aggregator -> aggregator.resolveIfNeeded(resolver)
    ));
    this.postAggregator = postAggregator;

    if (aggregatorFactories.isEmpty()) {
      Preconditions.checkArgument(postAggregator != null, "postAggregator must be present if there are no aggregators");
    }

    if (postAggregator == null) {
      Preconditions.checkArgument(aggregatorFactories.size() == 1, "aggregatorFactories.size == 1");
      outputName = Iterables.getOnlyElement(aggregatorFactories).getName();
      outputType = Iterables.getOnlyElement(aggregatorFactories).getOutputType();
    } else {
      // Verify that there are no "useless" fields in the aggregatorFactories.
      // Don't verify that the PostAggregator inputs are all present; they might not be.
      final Map<String, ValueDesc> overrides = Maps.newHashMap();
      final Set<String> dependentFields = postAggregator.getDependentFields();
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        if (!dependentFields.contains(aggregatorFactory.getName())) {
          throw new IAE("Unused field[%s] in Aggregation", aggregatorFactory.getName());
        }
        overrides.put(aggregatorFactory.getName(), aggregatorFactory.getOutputType());
      }
      outputName = postAggregator.getName();
      outputType = postAggregator.resolve(TypeResolver.override(sourceRowSignature, overrides));
    }

    // Verify that all "internal" aggregator names are prefixed by the output name of this aggregation.
    // This is a sanity check to make sure callers are behaving as they should be.

    for (VirtualColumn virtualColumn : virtualColumns) {
      if (!virtualColumn.getOutputName().startsWith(outputName)) {
        throw new IAE("VirtualColumn[%s] not prefixed under[%s]", virtualColumn.getOutputName(), outputName);
      }
    }

    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      if (!aggregatorFactory.getName().startsWith(outputName)) {
        throw new IAE("Aggregator[%s] not prefixed under[%s]", aggregatorFactory.getName(), outputName);
      }
    }
  }

  public static Aggregation create(RowSignature rowSignature, AggregatorFactory aggregatorFactory)
  {
    return new Aggregation(rowSignature, ImmutableList.of(), ImmutableList.of(aggregatorFactory), null);
  }

  public static Aggregation create(RowSignature rowSignature, PostAggregator postAggregator)
  {
    return new Aggregation(rowSignature, ImmutableList.of(), ImmutableList.of(), postAggregator);
  }

  public static Aggregation create(
      RowSignature rowSignature, List<AggregatorFactory> aggregatorFactories, PostAggregator postAggregator
  )
  {
    return new Aggregation(rowSignature, ImmutableList.of(), aggregatorFactories, postAggregator);
  }

  public static Aggregation create(
      RowSignature rowSignature, List<VirtualColumn> virtualColumns, AggregatorFactory aggregatorFactory
  )
  {
    return new Aggregation(rowSignature, virtualColumns, ImmutableList.of(aggregatorFactory), null);
  }

  public static Aggregation create(
      RowSignature rowSignature,
      List<VirtualColumn> virtualColumns,
      List<AggregatorFactory> aggregatorFactories,
      PostAggregator postAggregator
  )
  {
    return new Aggregation(rowSignature, virtualColumns, aggregatorFactories, postAggregator);
  }

  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  public List<AggregatorFactory> getAggregatorFactories()
  {
    return aggregatorFactories;
  }

  @Nullable
  public PostAggregator getPostAggregator()
  {
    return postAggregator;
  }

  public String getOutputName()
  {
    return outputName;
  }

  public ValueDesc getOutputType()
  {
    return outputType;
  }

  public Aggregation filter(final RowSignature sourceRowSignature, final DimFilter filter)
  {
    if (filter == null) {
      return this;
    }

    if (postAggregator != null) {
      // Verify that this Aggregation contains all input to its postAggregator.
      // If not, this "filter" call won't work right.
      final Set<String> dependentFields = postAggregator.getDependentFields();
      final Set<String> aggregatorNames = new HashSet<>();
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        aggregatorNames.add(aggregatorFactory.getName());
      }
      for (String field : dependentFields) {
        if (!aggregatorNames.contains(field)) {
          throw new ISE("Cannot filter an Aggregation that does not contain its inputs: %s", this);
        }
      }
    }

    final DimFilter baseOptimizedFilter = Filtration.create(filter)
                                                    .optimizeFilterOnly(sourceRowSignature)
                                                    .getDimFilter();

    final List<AggregatorFactory> newAggregators = new ArrayList<>();
    for (AggregatorFactory agg : aggregatorFactories) {
      if (agg instanceof FilteredAggregatorFactory) {
        final FilteredAggregatorFactory filteredAgg = (FilteredAggregatorFactory) agg;
        newAggregators.add(
            new FilteredAggregatorFactory(
                filteredAgg.getAggregator(),
                Filtration.create(DimFilters.and(filteredAgg.getFilter(), baseOptimizedFilter))
                          .optimizeFilterOnly(sourceRowSignature)
                          .getDimFilter()
            )
        );
      } else {
        newAggregators.add(new FilteredAggregatorFactory(agg, baseOptimizedFilter));
      }
    }

    return new Aggregation(sourceRowSignature, virtualColumns, newAggregators, postAggregator);
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
    final Aggregation that = (Aggregation) o;
    return Objects.equals(virtualColumns, that.virtualColumns) &&
           Objects.equals(aggregatorFactories, that.aggregatorFactories) &&
           Objects.equals(postAggregator, that.postAggregator);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, aggregatorFactories, postAggregator);
  }

  @Override
  public String toString()
  {
    return "Aggregation{" +
           "virtualColumns=" + virtualColumns +
           ", aggregatorFactories=" + aggregatorFactories +
           ", postAggregator=" + postAggregator +
           '}';
  }
}
