/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.guava;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.Pair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class MergeIterable<T> implements Iterable<T>
{
  private final Comparator<T> comparator;
  private final Iterable<Iterable<T>> baseIterables;

  public MergeIterable(
      Comparator<T> comparator,
      Iterable<Iterable<T>> baseIterables
  )
  {
    this.comparator = comparator;
    this.baseIterables = baseIterables;
  }

  @Override
  public Iterator<T> iterator()
  {
    List<Iterator<T>> iterators = Lists.newArrayList();
    for (Iterable<T> baseIterable : baseIterables) {
      iterators.add(baseIterable.iterator());
    }

    return new MergeIterator<>(comparator, iterators);
  }

  public static class Tagged<T> extends MergeIterable<Pair<T, Integer>>
  {
    public Tagged(Comparator<T> comparator, List<Iterable<T>> baseIterables)
    {
      super(Ordering.from(comparator).onResultOf(Pair.lhsFn()), withIndex(baseIterables));
    }
  }

  private static <T> List<Iterable<Pair<T, Integer>>> withIndex(List<Iterable<T>> iterables)
  {
    List<Iterable<Pair<T, Integer>>> withIndex = Lists.newArrayList();
    for (int i = 0; i < iterables.size(); i++) {
      final int index = i;
      final Iterable<T> iterable = iterables.get(i);
      withIndex.add(new Iterable<Pair<T, Integer>>()
      {
        @Override
        public Iterator<Pair<T, Integer>> iterator()
        {
          return Iterators.transform(iterable.iterator(), new Function<T, Pair<T, Integer>>()
          {
            @Override
            public Pair<T, Integer> apply(T input)
            {
              return Pair.of(input, index);
            }
          });
        }
      });
    }
    return withIndex;
  }
}
