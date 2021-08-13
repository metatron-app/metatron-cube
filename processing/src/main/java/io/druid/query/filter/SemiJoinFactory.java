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
import com.google.common.collect.Sets;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.segment.StringArray;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SemiJoinFactory
{
  public static DimFilter from(List<String> fieldNames, Sequence<Object[]> fieldValues)
  {
    return from(fieldNames, fieldValues, true);
  }

  public static DimFilter from(List<String> fieldNames, Iterator<Object[]> iterator)
  {
    return from(fieldNames, iterator, true);
  }

  public static DimFilter from(List<String> fieldNames, Sequence<Object[]> fieldValues, boolean allowDuplication)
  {
    return from(fieldNames, Sequences.toIterator(fieldValues), allowDuplication);
  }

  public static DimFilter from(List<String> fieldNames, Iterator<Object[]> iterator, boolean allowDuplication)
  {
    try {
      if (fieldNames.size() == 1) {
        final Set<String> set = Sets.newTreeSet();
        while (iterator.hasNext()) {
          if (!set.add(Objects.toString(iterator.next()[0], "")) && !allowDuplication) {
            return null;
          }
        }
        return new InDimFilter(fieldNames.get(0), null, ImmutableList.copyOf(set));
      } else {
        final Set<StringArray> set = Sets.newTreeSet();
        while (iterator.hasNext()) {
          if (!set.add(StringArray.of(iterator.next(), "")) && !allowDuplication) {
            return null;
          }
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
}

