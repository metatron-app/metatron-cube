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

package io.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.Pair;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpec;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 */
public interface RowSignature extends TypeResolver
{
  List<String> getColumnNames();

  List<ValueDesc> getColumnTypes();

  int size();

  default Iterable<Pair<String, ValueDesc>> columnAndTypes()
  {
    return GuavaUtils.zip(getColumnNames(), getColumnTypes());
  }

  default Iterable<Pair<String, ValueDesc>> dimensionAndTypes()
  {
    return columnAndTypes(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return input.isDimension();}
    });
  }

  default Iterable<Pair<String, ValueDesc>> metricAndTypes()
  {
    return columnAndTypes(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return !input.isDimension();}
    });
  }

  default List<Pair<String, ValueDesc>> columnAndTypes(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<Pair<String, ValueDesc>> columnAndTypes = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        columnAndTypes.add(Pair.of(columnNames.get(i), columnTypes.get(i)));
      }
    }
    return columnAndTypes;
  }

  default List<String> getDimensionNames()
  {
    return columnName(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return input.isDimension();}
    });
  }

  default List<String> getMetricNames()
  {
    return columnName(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return !input.isDimension();}
    });
  }

  default List<String> columnName(Predicate<ValueDesc> predicate)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();
    List<String> predicated = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      if (predicate.apply(columnTypes.get(i))) {
        predicated.add(columnNames.get(i));
      }
    }
    return predicated;
  }

  default List<ValueDesc> getDimensionTypes()
  {
    return columnTypes(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return input.isDimension();}
    });
  }

  default List<ValueDesc> getMetricTypes()
  {
    return columnTypes(new Predicate<ValueDesc>()
    {
      @Override
      public boolean apply(ValueDesc input) { return !input.isDimension();}
    });
  }

  default List<ValueDesc> columnTypes(Predicate<ValueDesc> predicate)
  {
    List<ValueDesc> columnTypes = getColumnTypes();
    List<ValueDesc> predicated = Lists.newArrayList();
    for (ValueDesc type : getColumnTypes()) {
      if (predicate.apply(type)) {
        predicated.add(type);
      }
    }
    return predicated;
  }

  // for streaming sub query.. we don't have index
  default RowSignature replaceDimensionToString()
  {
    List<ValueDesc> replaced = Lists.newArrayList(getColumnTypes());
    for (int i = 0; i < replaced.size(); i++) {
      if (ValueDesc.isDimension(replaced.get(i))) {
        replaced.set(i, ValueDesc.STRING);
      }
    }
    return new Simple(getColumnNames(), replaced);
  }

  default List<ValueDesc> tryColumnTypes(List<String> columns)
  {
    List<ValueDesc> columnTypes = Lists.newArrayList();
    for (String column : columns) {
      ValueDesc resolved = resolve(column);
      if (resolved == null || resolved.isUnknown()) {
        return null;
      }
      columnTypes.add(resolved);
    }
    return columnTypes;
  }

  default RowSignature retain(List<String> retainers)
  {
    List<String> columnNames = getColumnNames();
    List<ValueDesc> columnTypes = getColumnTypes();

    List<String> newColumnNames = Lists.newArrayList();
    List<ValueDesc> newColumnTypes = Lists.newArrayList();
    for (String retainer : retainers) {
      final int index = columnNames.indexOf(retainer);
      Preconditions.checkArgument(index >= 0, "cannot find column %s from %s", retainer, columnNames);
      newColumnNames.add(columnNames.get(index));
      newColumnTypes.add(columnTypes.get(index));
    }
    return new Simple(newColumnNames, newColumnTypes);
  }

  // todo remove this fuck
  default RowSignature resolve(Query<?> query, boolean finalzed)
  {
    List<String> newColumnNames = Lists.newArrayList();
    List<ValueDesc> newColumnTypes = Lists.newArrayList();

    TypeResolver resolver = RowResolver.of(this, BaseQuery.getVirtualColumns(query));

    if (query instanceof Query.ColumnsSupport) {
      final List<String> columns = ((Query.ColumnsSupport<?>) query).getColumns();
      for (String column : columns) {
        newColumnNames.add(column);
        newColumnTypes.add(resolver.resolve(column));
      }
      return new RowSignature.Simple(newColumnNames, newColumnTypes);
    }
    for (DimensionSpec dimensionSpec : BaseQuery.getDimensions(query)) {
      newColumnNames.add(dimensionSpec.getOutputName());
      newColumnTypes.add(dimensionSpec.resolve(resolver));
    }
    for (String metric : BaseQuery.getMetrics(query)) {
      newColumnNames.add(metric);
      newColumnTypes.add(resolver.resolve(metric));
    }
    List<AggregatorFactory> aggregators = BaseQuery.getAggregators(query);
    List<PostAggregator> postAggregators = BaseQuery.getPostAggregators(query);
    for (AggregatorFactory metric : aggregators) {
      metric = metric.resolveIfNeeded(Suppliers.ofInstance(resolver));
      newColumnNames.add(metric.getName());
      newColumnTypes.add(finalzed ? metric.finalizedType() : metric.getOutputType());
    }
    for (PostAggregator postAggregator : PostAggregators.decorate(postAggregators, aggregators)) {
      newColumnNames.add(postAggregator.getName());
      newColumnTypes.add(postAggregator.resolve(resolver));
    }
    return new RowSignature.Simple(newColumnNames, newColumnTypes);
  }

  default String asTypeString()
  {
    final StringBuilder s = new StringBuilder();
    for (Pair<String, ValueDesc> pair : columnAndTypes()) {
      if (s.length() > 0) {
        s.append(',');
      }
      s.append(pair.lhs).append(':').append(ValueDesc.toTypeString(pair.rhs));
    }
    return s.toString();
  }

  class Simple implements RowSignature
  {
    public static RowSignature fromTypeString(String typeString)
    {
      List<String> columnNames = Lists.newArrayList();
      List<ValueDesc> columnTypes = Lists.newArrayList();
      for (String column : StringUtils.split(typeString, ',')) {
        final int index = column.indexOf(':');
        columnNames.add(column.substring(0, index));
        columnTypes.add(ValueDesc.fromTypeString(column.substring(0, index)));
      }
      return new Simple(columnNames, columnTypes);
    }

    protected final List<String> columnNames;
    protected final List<ValueDesc> columnTypes;

    public Simple(List<String> columnNames, List<ValueDesc> columnTypes)
    {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
    }

    @Override
    public List<String> getColumnNames()
    {
      return columnNames;
    }

    @Override
    public List<ValueDesc> getColumnTypes()
    {
      return columnTypes;
    }

    @Override
    public int size()
    {
      return columnTypes.size();
    }

    @Override
    public ValueDesc resolve(String column)
    {
      int index = columnNames.indexOf(column);
      return index >= 0 ? columnTypes.get(index) : null;
    }
  }
}
