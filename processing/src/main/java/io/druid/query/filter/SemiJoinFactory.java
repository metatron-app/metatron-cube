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

package io.druid.query.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.RowExploder;
import io.druid.segment.StringArray;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SemiJoinFactory
{
  public static DimFilter from(List<String> fieldNames, Sequence<Object[]> fieldValues)
  {
    return from(fieldNames, Sequences.toIterator(fieldValues));
  }

  public static DimFilter from(List<String> fieldNames, Iterator<Object[]> iterator)
  {
    try {
      if (fieldNames.size() == 1) {
        boolean hasDuplication = false;
        final Set<String> set = Sets.newTreeSet();
        while (iterator.hasNext()) {
          set.add(Objects.toString(iterator.next()[0], ""));
        }
        return new InDimFilter(fieldNames.get(0), null, ImmutableList.copyOf(set));
      } else {
        final Set<StringArray> set = Sets.newTreeSet();
        while (iterator.hasNext()) {
          set.add(StringArray.of(iterator.next(), ""));
        }
        List<List<String>> valuesList = Lists.newArrayList();
        for (int i = 0; i < fieldNames.size(); i++) {
          valuesList.add(Lists.newArrayList());
        }
        for (StringArray array : set) {
          for (int i = 0; i < fieldNames.size(); i++) {
            valuesList.get(i).add(array.get(i));
          }
        }
        return new InDimsFilter(fieldNames, valuesList);
      }
    }
    finally {
      if (iterator instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) iterator);
      }
    }
  }

  public static Pair<DimFilter, RowExploder> extract(
      List<String> fieldNames, Sequence<Object[]> sequence, boolean allowDuplication
  )
  {
    return allowDuplication ? Pair.of(from(fieldNames, sequence), null) : extract(fieldNames, sequence);
  }

  public static Pair<DimFilter, RowExploder> extract(List<String> fieldNames, Sequence<Object[]> sequence)
  {
    final CloseableIterator<Object[]> iterator = Sequences.toIterator(sequence);
    try {
      if (fieldNames.size() == 1) {
        boolean hasDuplication = false;
        final Object2IntMap<String> mapping = new Object2IntAVLTreeMap<>();
        while (iterator.hasNext()) {
          mapping.computeInt(Objects.toString(iterator.next()[0], ""), (k, v) -> v == null ? 1 : v + 1);
        }
        DimFilter filter = new InDimFilter(fieldNames.get(0), null, ImmutableList.copyOf(mapping.keySet()));
        Map<String, Integer> duplications = Maps.newHashMap(Maps.filterEntries(mapping, e -> e.getValue() > 1));
        if (!duplications.isEmpty()) {
          return Pair.of(filter, new RowExploder(fieldNames, duplications, null));
        }
        return Pair.of(filter, null);
      } else {
        final Object2IntMap<StringArray> mapping = new Object2IntAVLTreeMap<>();
        while (iterator.hasNext()) {
          mapping.computeInt(StringArray.of(iterator.next(), ""), (k, v) -> v == null ? 1 : v + 1);
        }
        List<List<String>> valuesList = Lists.newArrayList();
        for (int i = 0; i < fieldNames.size(); i++) {
          valuesList.add(Lists.newArrayList());
        }
        for (StringArray array : mapping.keySet()) {
          for (int i = 0; i < fieldNames.size(); i++) {
            valuesList.get(i).add(array.get(i));
          }
        }
        DimFilter filter = new InDimsFilter(fieldNames, valuesList);
        StringArray.IntMap duplications = new StringArray.IntMap(Maps.filterEntries(mapping, e -> e.getValue() > 1));
        if (!duplications.isEmpty()) {
          return Pair.of(filter, new RowExploder(fieldNames, null, duplications));
        }
        return Pair.of(filter, null);
      }
    }
    finally {
      IOUtils.closeQuietly(iterator);
    }
  }
}

