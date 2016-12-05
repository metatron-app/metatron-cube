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

package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class VirtualColumns
{
  public static final VirtualColumns EMPTY = new VirtualColumns(
      ImmutableMap.<String, VirtualColumn>of()
  );

  public static VirtualColumns valueOf(VirtualColumn virtualColumn)
  {
    return virtualColumn == null ? EMPTY : valueOf(Arrays.asList(virtualColumn));
  }

  public static VirtualColumns valueOf(List<VirtualColumn> virtualColumns)
  {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return EMPTY;
    }
    Map<String, VirtualColumn> map = Maps.newHashMapWithExpectedSize(virtualColumns.size());
    for (VirtualColumn vc : virtualColumns) {
      map.put(vc.getOutputName(), vc.duplicate());
    }
    return new VirtualColumns(map);
  }

  public static DimensionSelector toDimensionSelector(final ObjectColumnSelector selector)
  {
    if (selector == null) {
      return new NullDimensionSelector();
    }

    return new DimensionSelector()
    {
      private int counter;
      private final Map<String, Integer> valToId = Maps.newHashMap();
      private final List<String> idToVal = Lists.newArrayList();

      @Override
      public IndexedInts getRow()
      {
        Object selected = selector.get();
        String value = selected == null ? null : String.valueOf(selected);
        Integer index = valToId.get(value);
        if (index == null) {
          valToId.put(value, index = counter++);
          idToVal.add(value);
        }
        final int result = index;
        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return 1;
          }

          @Override
          public int get(int index)
          {
            return result;
          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill");
          }

          @Override
          public void close() throws IOException
          {
          }

          @Override
          public Iterator<Integer> iterator()
          {
            return Iterators.singletonIterator(result);
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public String lookupName(int id)
      {
        return idToVal.get(id);
      }

      @Override
      public int lookupId(String name)
      {
        return valToId.get(name);
      }
    };
  }

  public static DimensionSelector toFixedDimensionSelector(List<String> values)
  {
    final int[] index = new int[values.size()];
    final Map<String, Integer> valToId = Maps.newHashMap();
    final List<String> idToVal = Lists.newArrayList();
    for (int i = 0; i < index.length; i++) {
      String value = values.get(i);
      Integer id = valToId.get(value);
      if (id == null) {
        valToId.put(value, id = idToVal.size());
        idToVal.add(value);
      }
      index[i] = id;
    }
    final int cardinality = idToVal.size();
    final IndexedInts indexedInts = new ArrayBasedIndexedInts(index);

    return new DimensionSelector() {

      @Override
      public IndexedInts getRow()
      {
        return indexedInts;
      }

      @Override
      public int getValueCardinality()
      {
        return cardinality;
      }

      @Override
      public String lookupName(int id)
      {
        return id >= 0 && id < cardinality ? idToVal.get(id) : null;
      }

      @Override
      public int lookupId(String name)
      {
        return valToId.get(name);
      }
    };
  }

  public static class VirtualColumnAsColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final VirtualColumn virtualColumn;
    private final ColumnSelectorFactory factory;
    private final String dimensionColumn;
    private final Set<String> metricColumns;

    public VirtualColumnAsColumnSelectorFactory(
        VirtualColumn virtualColumn,
        ColumnSelectorFactory factory,
        String dimensionColumn,
        Set<String> metricColumns
    )
    {
      this.virtualColumn = virtualColumn;
      this.factory = factory;
      this.dimensionColumn = dimensionColumn;
      this.metricColumns = metricColumns;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return factory.getColumnNames();
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      if (dimensionColumn.equals(dimensionSpec.getDimension())) {
        return virtualColumn.asDimension(dimensionSpec.getDimension(), factory);
      }
      return factory.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asFloatMetric(columnName, factory);
      }
      return factory.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asDoubleMetric(columnName, factory);
      }
      return factory.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asLongMetric(columnName, factory);
      }
      return factory.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        return virtualColumn.asMetric(columnName, factory);
      }
      return factory.makeObjectColumnSelector(columnName);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      for (String required : Parser.findRequiredBindings(expression)) {
        if (metricColumns.contains(required)) {
          throw new UnsupportedOperationException("makeMathExpressionSelector");
        }
      }
      return factory.makeMathExpressionSelector(expression);
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      if (metricColumns.contains(columnName)) {
        throw new UnsupportedOperationException("getColumnCapabilities");
      }
      return factory.getColumnCapabilities(columnName);
    }
  }

  private final Map<String, VirtualColumn> virtualColumns;

  public VirtualColumns(Map<String, VirtualColumn> virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  public VirtualColumn getVirtualColumn(String dimension)
  {
    int index = dimension.indexOf('.');
    return virtualColumns.get(index < 0 ? dimension : dimension.substring(0, index));
  }
}
