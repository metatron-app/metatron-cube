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

package io.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatchers;
import io.druid.segment.data.IndexedID;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;

import java.util.Map;
import java.util.Set;

public class MVIteratingSelector extends DelegatedDimensionSelector implements Supplier<IndexedID>
{
  private final MVIterator iterator;
  private ValueMatcher matcher;

  public static MVIteratingSelector wrap(DimensionSelector selector)
  {
    return new MVIteratingSelector(selector);
  }

  private MVIteratingSelector(DimensionSelector delegate)
  {
    super(delegate);
    this.iterator = new MVIterator();
    this.matcher = null;
  }

  @Override
  public IndexedInts getRow()
  {
    return iterator.update(super.getRow());
  }

  public void augment(ValueMatcher matcher)
  {
    this.matcher = ValueMatchers.and(this.matcher, matcher);
  }

  public ObjectColumnSelector asObjectSelector()
  {
    return ColumnSelectors.asSelector(ValueDesc.INDEXED_ID, this);
  }

  @Override
  public IndexedID get()
  {
    return iterator;
  }

  private class MVIterator implements IndexedID, IndexedInts
  {
    private int index;
    private IndexedInts indexedInts;

    @Override
    public int size()
    {
      return indexedInts.size();
    }

    @Override
    public int get(int index)
    {
      return indexedInts.get(this.index = index);
    }

    public IndexedInts update(final IndexedInts current)
    {
      index = 0;
      indexedInts = current;
      if (matcher != null) {
        int[] rewritten = new int[current.size()];
        for (; index < rewritten.length; index++) {
          rewritten[index] = matcher.matches() ? current.get(index) : -1;
        }
        indexedInts = IndexedInts.from(rewritten);
      }
      return this;
    }

    @Override
    public int get()
    {
      return indexedInts.get(index);
    }

    @Override
    public int lookupId(String name)
    {
      return MVIteratingSelector.this.lookupId(name);
    }

    @Override
    public Object lookupName(int id)
    {
      return MVIteratingSelector.this.lookupName(id);
    }

    @Override
    public ValueType elementType()
    {
      return ValueType.STRING;
    }
  }

  public static void rewrite(ColumnSelectorFactory factory, Map<String, MVIteratingSelector> mvMap, DimFilter filter)
  {
    final DimFilter rewritten = DimFilters.rewrite(
        filter, f -> GuavaUtils.containsAny(Filters.getDependents(f), mvMap.keySet()) ? f : null
    );
    final Set<String> dependents = Filters.getDependents(rewritten);
    if (!dependents.isEmpty()) {
      final ValueMatcher matcher = toMatcher(factory, rewritten, dependents, mvMap);
      for (String dependent : dependents) {
        mvMap.get(dependent).augment(matcher);
      }
    }
  }

  public static ValueMatcher toMatcher(
      ColumnSelectorFactory factory,
      DimFilter filter,
      Set<String> dependents,
      Map<String, MVIteratingSelector> mvMap
  )
  {
    final Map<String, ObjectColumnSelector> hacked = Maps.newHashMap();
    for (String dependent : dependents) {
      hacked.put(dependent, mvMap.get(dependent).asObjectSelector());
    }
    return Filters.toFilter(filter, factory).makeMatcher(
        new ColumnSelectorFactories.Delegated(factory)
        {
          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            return hacked.containsKey(columnName) ? hacked.get(columnName) : super.makeObjectColumnSelector(columnName);
          }

          @Override
          public ValueDesc resolve(String columnName)
          {
            return hacked.containsKey(columnName) ? ValueDesc.INDEXED_ID : super.resolve(columnName);
          }
        }
    );
  }
}
