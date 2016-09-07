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
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
