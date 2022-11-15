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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.druid.common.IntTagged;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.RowExploder;
import io.druid.segment.StringArray;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SemiJoinFactory
{
  public static DimFilter from(List<String> fieldNames, Sequence<Object[]> fieldValues)
  {
    return from(fieldNames, Sequences.toIterator(fieldValues));
  }

  public static DimFilter from(List<String> fieldNames, Iterable<Object[]> iterable)
  {
    return from(fieldNames, iterable.iterator());
  }

  public static DimFilter from(List<String> fieldNames, Iterator<Object[]> iterator)
  {
    try {
      if (fieldNames.size() == 1) {
        final List<String> set = Lists.newArrayList();
        while (iterator.hasNext()) {
          set.add(Objects.toString(iterator.next()[0], ""));
        }
        Collections.sort(set);
        return toInFilter(fieldNames.get(0), GuavaUtils.dedup(set));
      } else {
        final List<StringArray> set = Lists.newArrayList();
        while (iterator.hasNext()) {
          set.add(StringArray.of(iterator.next(), ""));
        }
        Collections.sort(set);
        return toInsFilter(fieldNames, GuavaUtils.dedup(set));
      }
    }
    finally {
      if (iterator instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) iterator);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static DimFilter toFilter(List<String> fieldNames, Object2IntSortedMap<?> mapping)
  {
    Iterator<?> iterator = mapping.keySet().iterator();
    if (fieldNames.size() == 1) {
      return toInFilter(fieldNames.get(0), (Iterator<String>) iterator);
    } else {
      return toInsFilter(fieldNames, (Iterator<StringArray>) iterator);
    }
  }

  public static Iterator<?> toKeys(Object2IntSortedMap<?> mapping, int i)
  {
    return mapping.object2IntEntrySet().stream()
                  .filter(e -> e.getIntValue() == i)
                  .map(e -> e.getKey())
                  .iterator();
  }

  private static DimFilter toInFilter(String fieldName, Iterator<String> set)
  {
    List<String> values = ImmutableList.<String>copyOf(set);
    Hasher hasher = Hashing.murmur3_128().newHasher();
    values.forEach(v -> hasher.putUnencodedChars(v));
    return new InDimFilter(fieldName, null, values, hasher.hash().asBytes());
  }

  private static InDimsFilter toInsFilter(List<String> fieldNames, Iterator<StringArray> objects)
  {
    List<List<String>> valuesList = Lists.newArrayList();
    for (int i = 0; i < fieldNames.size(); i++) {
      valuesList.add(Lists.newArrayList());
    }
    Hasher hasher = Hashing.murmur3_128().newHasher();
    while (objects.hasNext()) {
      StringArray array = objects.next();
      for (int i = 0; i < fieldNames.size(); i++) {
        valuesList.get(i).add(array.get(i));
        hasher.putUnencodedChars(array.get(i));
      }
    }
    return new InDimsFilter(fieldNames, valuesList, hasher.hash().asBytes());
  }

  public static int sizeOf(DimFilter filter)
  {
    if (filter instanceof InDimFilter) {
      return ((InDimFilter) filter).getValues().size();
    } else if (filter instanceof InDimsFilter) {
      return ((InDimsFilter) filter).getValues().get(0).size();
    }
    return -1;
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
      Hasher hasher = Hashing.murmur3_128().newHasher();
      if (fieldNames.size() == 1) {
        boolean hasDuplication = false;
        final Object2IntMap<String> mapping = new Object2IntAVLTreeMap<>();
        while (iterator.hasNext()) {
          String key = Objects.toString(iterator.next()[0], "");
          mapping.computeInt(key, (k, v) -> v == null ? 1 : v + 1);
          hasher.putUnencodedChars(key);
        }
        ImmutableList<String> values = ImmutableList.copyOf(mapping.keySet());
        DimFilter filter = new InDimFilter(fieldNames.get(0), null, values, hasher.hash().asBytes());
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
            hasher.putUnencodedChars(array.get(i));
          }
        }
        DimFilter filter = new InDimsFilter(fieldNames, valuesList, hasher.hash().asBytes());
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

  public static IntTagged<Object2IntSortedMap<?>> toMap(int dims, Iterator<Object[]> iterator)
  {
    try {
      if (dims == 1) {
        IntTagged<Object2IntSortedMap<String>> v = singleCounter(iterator);
        return IntTagged.of(v.tag, v.value());
      } else {
        IntTagged<Object2IntSortedMap<StringArray>> v = multiCounter(iterator);
        return IntTagged.of(v.tag, v.value());
      }
    }
    finally {
      if (iterator instanceof Closeable) {
        IOUtils.closeQuietly((Closeable) iterator);
      }
    }
  }

  private static IntTagged<Object2IntSortedMap<String>> singleCounter(Iterator<Object[]> iterator)
  {
    int counter = 0;
    Object2IntSortedMap<String> mapping = new Object2IntAVLTreeMap<>();
    while (iterator.hasNext()) {
      mapping.computeInt(Objects.toString(iterator.next()[0], ""), (k, v) -> v == null ? 1 : v + 1);
      counter++;
    }
    return IntTagged.of(counter, mapping);
  }

  private static IntTagged<Object2IntSortedMap<StringArray>> multiCounter(Iterator<Object[]> iterator)
  {
    int counter = 0;
    Object2IntSortedMap<StringArray> mapping = new Object2IntAVLTreeMap<>();
    while (iterator.hasNext()) {
      mapping.computeInt(StringArray.of(iterator.next(), ""), (k, v) -> v == null ? 1 : v + 1);
      counter++;
    }
    return IntTagged.of(counter, mapping);
  }
}

